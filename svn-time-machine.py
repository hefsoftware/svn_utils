##   Copyright [2024] [Marzocchi Alessandro]
##
##   Licensed under the Apache License, Version 2.0 (the "License");
##   you may not use this file except in compliance with the License.
##   You may obtain a copy of the License at
##
##       http://www.apache.org/licenses/LICENSE-2.0
##
##   Unless required by applicable law or agreed to in writing, software
##   distributed under the License is distributed on an "AS IS" BASIS,
##   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##   See the License for the specific language governing permissions and
##   limitations under the License.

import sys
import svn
import argparse
import tempfile
import urllib
import os

# Helper function for checking out an external at a particular revision
# The passed path MUST contain an external that was already updated.
def checkoutTimeMachineExternal(entry: svn.ExternalFullInfo, rootRepository: str, internalRevision: str, externalDate: str):
  sys.stdout.write(f"  External: {entry.url}@{entry.revision}\n")
  svn.checkout(entry.fullPath, entry.fullUrl, revision=entry.revision, ignoreExternal=True)
  externals=svn.getExternals(entry.fullPath)
  for d in externals.listDirs():
    newVersions=d.map(svn.mapExternalBefore(rootRepository, internalRevision, externalDate))
    svn.setExternals(newVersions)
    for e in newVersions.listFull():
      checkoutTimeMachineExternal(e, rootRepository, internalRevision, externalDate)

# Checkout url to path with a revision <= maxRevision
# maxRevision may be either an integer or a SVN date/time {2024-07-11}
# Maximum revision of externals will be:
# - If useRootRevisionAsMax is True:
#   Externals on the same repository: The revision of root (which may be lower than maxRevision)
#   Externals on another repository: the timestamp of the commit of root for 
# - If useRootRevisionAsMax is False and maxRevision is a number:
#   Externals on the same repository: maxRevision (which may be higher than the revision of the root)
#   Externals on another repository: the timestamp of the commit maxRevision
# - If useRootRevisionAsMax is False and maxRevision is a date/time:
#   All externals: The timestamp of maxRevision
# If recurse is true the process will be replied also for all externals of the project, otherwise it will stop at first level of externals (used to create a shallow tag)
def checkoutTimeMachine(url, path, maxRevision, *, action, useRootRevisionAsMax=True, recurse=True):
  repository=svn.getRepositoryRoot(url)
  commitData=svn.getCommit(repository, maxRevision)
  rootCommitData=svn.getCommitBefore(url, maxRevision)
  if useRootRevisionAsMax:
    internalRevision=rootCommitData.revision
    externalDate=rootCommitData.date
  else:
    if commitData.revision!=rootCommitData.revision:
      sys.stdout.write(f"WARNING!\n")
      sys.stdout.write(f"  Checkout '{url}' has revision {rootCommitData.revision}.\n")
      sys.stdout.write(f"  Checkout of externals may have a revision up to {commitData.revision}.\n")
    if maxRevision.startswith("{"):
      internalRevision=commitData.revision
      externalDate=maxRevision[1:-1]
    else:
      internalRevision=commitData.revision
      externalDate=commitData.date
  printedCommit=rootCommitData if useRootRevisionAsMax else commitData
  sys.stdout.write(f"+-%s--+\n"%("-"*max(40,len(url)),))
  sys.stdout.write(f"| TIME MACHINE {action} OF:\n")
  sys.stdout.write(f"|  {url}\n")
  sys.stdout.write(f"+-%s--+\n"%("-"*max(40,len(url)),))
  sys.stdout.write(f"  COMMIT: {printedCommit.revision} [{svn.localTimeString(printedCommit.date)}]\n")
  sys.stdout.write(f"  AUTHOR: {printedCommit.author}\n")
  sys.stdout.write(f"  MESSAGE:\n")
  sys.stdout.write("    "+commitData.message.replace("\n","\n    ")+"\n")
  svn.checkout(path, url, revision=internalRevision, ignoreExternal=True)
  externals=svn.getExternals(path)
  for d in externals.listDirs():
    newVersions=d.map(svn.mapExternalBefore(repository, internalRevision, externalDate))
    svn.setExternals(newVersions)
    if recurse:
      for e in newVersions.listFull():
        checkoutTimeMachineExternal(e, repository, internalRevision, externalDate)

# Deep tag of url to another url path with a revision <= maxRevision
# See checkoutTimeMachine for a description of parameters
def tagTimeMachine(source, destination, maxRevision, *, message=None, useRootRevisionAsMax=True, enableImports=False):
  temporaryDir=os.path.normpath(os.path.abspath("temp"))
  copies=[]
  repository=svn.getRepositoryRoot(source)
  if not destination.startswith(repository+"/"):
    sys.stderr.write("ERROR! Destination '{destination}' must be in same repository as source '{source}'")
    sys.exit(1)
  commitData=svn.getCommit(repository, maxRevision)
  rootCommitData=svn.getCommitBefore(source, maxRevision)
  if useRootRevisionAsMax:
    internalRevision=rootCommitData.revision
    externalDate=rootCommitData.date
  else:
    if commitData.revision!=rootCommitData.revision:
      sys.stdout.write(f"WARNING!\n")
      sys.stdout.write(f"  Checkout '{source}' has revision {rootCommitData.revision}.\n")
      sys.stdout.write(f"  Checkout of externals may have a revision up to {commitData.revision}.\n")
    if maxRevision.startswith("{"):
      internalRevision=commitData.revision
      externalDate=maxRevision[1:-1]
    else:
      internalRevision=commitData.revision
      externalDate=commitData.date
  if message is None:
    message=f"Tag of revision {maxRevision}"
  curTemp=0
  def newTemporaryDir():
    nonlocal curTemp
    while True:
      try:
        ret=os.path.join(temporaryDir, f"{curTemp}")
        curTemp=curTemp+1
        os.mkdir(ret)
        return ret
      except FileExistsError:
        pass
  def isInternalUrl(url):
    return url.startswith(repository+"/")
  
  # Function to filter out complex externals (the ones that have external themselves)
  # dest is the root of the directory we are handling
  # messageList is a list of messages that will be used for the commit
  def filterOutComplex(dest: str, messageList: str):
    def ret(e: svn.ExternalFullInfo):
      # Also converts relative urls to full urls if we are referring to something outside repository
      if svn.getExternals(e.fullUrl, revision=e.revision):
        deltaPath=[]
        base=e.fullPath
        while base!=dest:
          base, name=os.path.split(base)
          deltaPath.insert(0,name)
        destUrl=destination
        for n in deltaPath:
          if not destUrl.endswith("/"):
            destUrl=destUrl+"/"
          destUrl=destUrl+urllib.parse.quote(n)
        if isInternalUrl(e.fullUrl):
          nextSteps.append((e.fullUrl, e.url, destUrl))
        else:
          handleExternalCheckout(e, dest, messageList)
      else:
        url=e.url if isInternalUrl(e.fullUrl) else e.fullUrl
        return svn.ExternalInfo(e.name, url, e.revision)
    return ret
  # d is a DirWithExternal that must be handled
  def handleDirWithExternals(d, dest, messageList):
    newVersions=d.map(svn.mapExternalBefore(repository, internalRevision, externalDate))
    newVersions=newVersions.map(filterOutComplex(dest, messageList))
    svn.setExternals(newVersions)
    
  def handleExternalCheckout(entry, dest, messageList):
    if not enableImports:
      sys.stdout.write(f"\n\nERROR!\nContent of external '{entry.fullUrl}' must be copied inside tag to perform the tag.\nUse --enable-imports to allow it or make a shallow copy.\n")
      sys.exit(0)
      
    def getLocalPath(url):
      relativePath=svn.getRelativePath(entry.fullUrl, url).split("/")
      return os.path.join(entry.fullPath, *map(lambda x: urllib.parse.unquote(x), relativePath))
    messageList.append(f"- Import of {entry.fullUrl}@{entry.revision}")
    sys.stdout.write(f"  IMPORT: {entry.fullUrl}@{entry.revision}\n")
    sys.stdout.write(f"    INTO: {entry.fullPath}\n")
    svn.export(entry.fullPath, entry.fullUrl, revision=entry.revision, ignoreExternal=True)
    svn.add(entry.fullPath)
    
    for d in svn.getExternals(entry.fullUrl, revision=entry.revision).listDirs():
      newDir=svn.DirWithExternals(getLocalPath(d.basePath), d.repository)
      for e in d.listFull():
        newDir.add(e.name, e.url, e.revision)
      handleDirWithExternals(newDir, dest, messageList)
  def handleInternalCheckout(url: str, showedUrl: str|None, destUrl: str):
    messageList=[message if showedUrl is None else message+f"\n- Tag of external {destUrl}"]
    dest=newTemporaryDir()
    isInternal=isInternalUrl(url)
    curRevision=svn.getCommitBefore(url, internalRevision if isInternal else "{%s}"%(externalDate,))
    if showedUrl is not None:
      sys.stdout.write(f"  EXTERNAL: {showedUrl}@{curRevision.revision}\n")
      sys.stdout.write(f"    DESTINATION: {destUrl}\n")
    else:
      sys.stdout.write(f"+-%s--+\n"%("-"*max(40,len(url)),))
      sys.stdout.write(f"| TIME MACHINE TAG OF:\n")
      sys.stdout.write(f"|  {url}\n")
      sys.stdout.write(f"+-%s--+\n"%("-"*max(40,len(url)),))
      sys.stdout.write(f"  COMMIT: {curRevision.revision} [{svn.localTimeString(curRevision.date)}]\n")
      sys.stdout.write(f"  AUTHOR: {curRevision.author}\n")
      sys.stdout.write(f"  MESSAGE:\n")
      sys.stdout.write("    "+curRevision.message.replace("\n","\n    ")+"\n")
      sys.stdout.write(f"  DESTINATION: {destUrl}\n")

    svn.checkout(dest, url, revision=internalRevision if isInternal else "{%s}"%(externalDate,), ignoreExternal=True)
    externals=svn.getExternals(dest)
    for d in externals.listDirs():
      handleDirWithExternals(d, dest, messageList)
    #curMessage=
    curMessage='\n'.join(messageList)
    #print(f"Copy {dest}->{destUrl}\n'{curMessage}'")
    copies.append((dest, destUrl, curMessage))
  # url, showed url, destination url
  nextSteps=[(source, None, destination)]
  while nextSteps:
    (url, showedUrl, destUrl)=nextSteps.pop()
    handleInternalCheckout(url, showedUrl, destUrl)
  sys.stdout.write(f"  PERFORMING COPY\n")
  for (dest, destUrl, curMessage) in copies:
    svn.copy(dest, destUrl, message=curMessage)

#svn.copy(

parser = argparse.ArgumentParser(
                    prog=os.path.basename(sys.argv[0]),
                    description='SVN utility to checkout or tag a repository as it was at a particular revision/timestamp (including the external that were used at that particular date or time).')

parser.add_argument('url', help="The url to checkout/tag")
parser.add_argument('revision', help="The time instant to checkout/tag. Can be an integer revision number (e.g. 437) or a date/time as defined by SVN (e.g. {2006-02-17}, {2006-02-17T15:30}, ...). If an integer revision is given the externals fetched will be the ones at the date/time of that commit, otherwise the ones at the date/time specified.")
subparsers = parser.add_subparsers(dest='command')
# Checkout
parserCheckout=subparsers.add_parser('checkout', help="Checkout of a time-machined repository to a particular path")
parserCheckout.add_argument('path', help="The destination for the checkout")
parserCheckout.add_argument('--use-root-revision', default=False, action=argparse.BooleanOptionalAction, help="If true all the externals will be with commit revisions/timestamps less or equal to the commit revision/timestamps of the root checkout directory (the one speciied with url)")
# Tag
parserDeepTag=subparsers.add_parser('tag', help="Tag of a time-machined repository. In case the externals contains more externals the operation will be split in multiple steps to ensure that even the sub-externals version is forced. In case the externals refers to another repository this may also mean that files from other repository will be actually imported inside the tag.")
parserDeepTag.add_argument('destination', help="The url destination for the tag")
parserDeepTag.add_argument('-r', '--use-root-revision', default=False, action=argparse.BooleanOptionalAction, help="If true all the externals will be with commit revisions/timestamps less or equal to the commit revision/timestamps of the root checkout directory (the one speciied with url)")
parserDeepTag.add_argument('-i', '--enable-imports', default=False, action=argparse.BooleanOptionalAction, help="If true externals containing externals themselves will be imported into the commited tag to make sure the content is exactly the same at the given time.")
parserDeepTag.add_argument('-m', '--message', default=None, help="The url destination for the tag")
# Shallow tag
parserShallowTag=subparsers.add_parser('shallow-tag', help="Tag of a time-machined repository. Note that this command will set explicit versions only on first level of externals. Use tag for having the generated tag to have also fixed revision for externals of externals.")
parserShallowTag.add_argument('destination', help="The url destination for the tag")
parserShallowTag.add_argument('-r', '--use-root-revision', default=False, action=argparse.BooleanOptionalAction, help="If true all the externals will be with commit revisions/timestamps less or equal to the commit revision/timestamps of the root checkout directory (the one speciied with url)")


args = parser.parse_args()
if args.command=='tag':
  tagTimeMachine(args.url, args.destination, args.revision, useRootRevisionAsMax=args.use_root_revision, message=args.message, enableImports=args.enable_imports)
  #checkoutTimeMachine(args.url, temporaryDir, args.revision, useRootRevisionAsMax=args.use_root_revision, action="DEEP TAG")
#if args.command=='checkout':
#  checkoutTimeMachine(args.url, args.path, args.revision, useRootRevisionAsMax=args.use_root_revision, action="CHECKOUT")
#elif args.command=="tag":
#  with tempfile.TemporaryDirectory() as temporaryDir:
#    checkoutTimeMachine(args.url, temporaryDir, args.revision, useRootRevisionAsMax=args.use_root_revision, recurse=False, action="TAG")
#    svn.copy(temporaryDir, args.destination, message=f"Tag of revision {args.revision}")
