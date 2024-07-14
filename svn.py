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


import subprocess
from dataclasses import field, dataclass, InitVar
import xml.etree.ElementTree as ET
import os.path
import urllib.parse
import re
import datetime
import time

def executeSvn(*commandLine):
  #print(commandLine)
  cmdLine=["svn"]
  cmdLine.extend(commandLine)
  result=subprocess.run(cmdLine, stdout=subprocess.PIPE)
  if result.returncode!=0:
    raise SvnException("FAILED TO RUN SVN '%s'"%(" ".join(commandLine)))
  return result.stdout

class SvnException(Exception):
    pass

@dataclass
class CommitInfo:
  revision: int
  date: str
  author: str
  message: str

def isUrl(path):
  return path.startswith("http://")

@dataclass(frozen=True)
class ExternalInfo:
  name: str
  url: str
  revision: str
  @property
  def isRelativeUrl(self):
    return self.url.startswith("^/")

# External info, including its repository
@dataclass(frozen=True)
class ExternalFullInfo(ExternalInfo):
  baseDir: str
  repository: str
  @property
  def fullPath(self):
    if isUrl(self.baseDir):
      return f"{self.baseDir}/{self.name}"
    else:
      return os.path.join(self.baseDir, self.name)
  @property
  def fullUrl(self):
    return repositoryJoin(self.repository, self.url)

# Join an url with a repository.
# If url starts with ^ it will be joined to repository, otherwise it will be the returned url
def repositoryJoin(repository, url):
  if url.startswith("^/"):
    url=url[2:]
  elif url.startswith("/"):
    url=url[1:]
  if not repository.endswith("/"):
    repository=repository+"/"
  return url if isUrl(url) else (repository+url)

# Joins an url or a file path
def pathJoin(base, extend):
  if isUrl(base):
    return repositoryJoin(base, extend)
  else:
    return os.path.join(base, extend)

def getRelativePath(base, dest):
  if isUrl(base):
    if not dest.startswith(base+"/"):
      raise SvnException(f"'{dest}' is not a descendant of '{base}'")
    return dest[len(base)+1:]
  else:
    raise SvnException("Non url are not handled for now")
class DirWithExternals:
  def __init__(self, basePath:str, repository: str):
    self.__basePath=basePath
    self.__repository=repository
    self.__data={}
  # Returns an iterator to elements as ExternalInfo structures
  def __iter__(self):
    return iter(self.__data.values())
  def __bool__(self):
    return len(self.__data)!=0
  @property
  def basePath(self):
    return self.__basePath
  @property
  def repository(self):
    return self.__repository
  # Returns an iterator to elements as ExternalFullInfo structures
  def listFull(self):
    for e in self:
      yield ExternalFullInfo(e.name, e.url, e.revision, self.__basePath, self.__repository)
  # Adds an entry
  def add(self, name: str, url: str, revision: str|None=None):
    self.__data[name]=ExternalInfo(name, url, revision)
  # Returns another DirWithExternals with elements created by calling convert(ExternalFullInfo)->ExternalFullInfo|ExternalInfo|None
  def map(self, conversionFunction):
    ret=DirWithExternals(self.basePath, self.repository)
    for e in self.listFull():
      val=conversionFunction(e)
      if val is not None:
        assert(isinstance(val, (ExternalInfo, ExternalFullInfo)))
        ret.add(val.name, val.url, val.revision)
    return ret

class ExternalTree:
  def __init__(self, localPath: str):
    self.__localPath=localPath
    self.__data={}
  def __iter__(self):
    return iter(self.__data.items())
  def __bool__(self):
    return bool(self.__data)
  def add(self, localPath: str, url: str, repository: str, commit: str|None=None):
    if isUrl(self.__localPath):
      localPath=repositoryJoin(self.__localPath, localPath)
      if not localPath.startswith(self.__localPath+"/"):
        raise SvnException(f"Url {localPath} is outside the root directory {self.__localPath}")
      split=localPath.rfind('/')
      baseDir,baseName=localPath[:split],localPath[split+1:]
    else:
      localPath=os.path.normpath(os.path.join(self.__localPath, localPath))
      baseDir, baseName=os.path.split(localPath)
    self.__data.setdefault(baseDir,DirWithExternals(baseDir, repository)).add(baseName, url, commit)
  # A list of DirWithExternals
  def listDirs(self):
    return iter(self.__data.values())
  # Returns an iterator of ExternalFullInfo with all the elements
  def listFull(self):
    ret=[]
    for (base, dirObject) in self:
      for entry in dirObject.listFull():
        yield entry
  #def externalBaseDirs(self):

    
# Get information about the commit happened at a time <=reference
def getCommitBefore(url: str, reference: str)->CommitInfo:
  cmdLine=['log', url, '--xml', '-r', f'{reference}:0', '-l', '1']
  result=executeSvn(*cmdLine)
  entry = ET.fromstring(result).find('logentry')
  if entry is None:
    raise SvnException("COULD NOT FIND COMMIT IN SVT XML")
  return CommitInfo(revision=int(entry.attrib.get('revision')), author=entry.findtext('author'), date=entry.findtext('date'), message=entry.findtext('msg'))

# Get information about the commit happened at a time <=reference
def getCommit(url: str, reference: str)->CommitInfo:
  cmdLine=['log', url, '--xml', '-r', f'{reference}', '-l', '1']
  result=executeSvn(*cmdLine)
  entry = ET.fromstring(result).find('logentry')
  if entry is None:
    raise SvnException("COULD NOT FIND COMMIT IN SVT XML")
  return CommitInfo(revision=int(entry.attrib.get('revision')), author=entry.findtext('author'), date=entry.findtext('date'), message=entry.findtext('msg'))

# Returns a dictionary with external dependencies
#   baseDir: [(name, url)]
def getExternals(path, *, recursive=True, revision=None):
  cmdLine=['propget', 'svn:externals', path, '--xml']
  if recursive:
    cmdLine.append('-R')
  if revision is not None:
    cmdLine.extend(('-r', str(revision)))
  result=executeSvn(*cmdLine)
  ret=ExternalTree(path)
  for e in ET.fromstring(result).iter("target"):
    base=e.attrib.get("path")
    repository=getRepositoryRoot(base)
    for l in e.findtext("property").split("\n"):
      l=l.strip()
      if not l:
        continue
      pos=l.find(" ")
      if pos<0:
        sys.stderr.write(f"INVALID EXTERNAL ENTRY: {l}")
        sys.exit(0)
      baseName=l[pos+1:]
      if baseName.startswith("\"") and baseName.endswith("\""):
        baseName=baseName[1:-1]
      url=l[:pos]
      explicitRev=re.match(r"(.*)@(\d+)", url)
      if explicitRev is not None:
        url=explicitRev.group(1)
        revision=explicitRev.group(2)
      else:
        revision=None
      ret.add(pathJoin(base, baseName), url, repository, revision)
  return ret

# values must be a DirWithExternals or a ExternalFullInfo
def setExternals(values):
  if isinstance(values, ExternalFullInfo):
    for d in values.listDirs():
      setExternals(d)
  elif isinstance(values, DirWithExternals):
    def generateLine(entry: ExternalInfo):
      ret=entry.url
      if entry.revision is not None:
        ret+=f"@{entry.revision}"
      ret+=" "
      if " " in entry.name:
        ret+=f'"{entry.name}"'
      else:
        ret+=entry.name
      return ret
    newExternal="\n".join(map(lambda it: generateLine(it),values))
    result=executeSvn('propset', 'svn:externals', newExternal, values.basePath)
  else:
    raise SvnError("setExternal expects a DirWithExternals or an ExternalFullInfo")

def getRepositoryRoot(path):
  result=executeSvn('info', path, '--xml')
  return ET.fromstring(result).find('entry').find('repository').findtext("root")

# Gets a functor that can be passed to DirWithExternals.map()
# rootRepository must be the url to a repository
# internalRevision can be a numeric revision or a date/time
# externalDate must be a date/time string
# Given an entry:
# - If the entry has a fixed revision returned revision will be its revision
# - If entry repository is same as rootRepository returned revision will be  <= internalRevision
# - If entry repository is not the same as rootRepository returned revision will be  <= externalDate
def mapExternalBefore(rootRepository, internalRevision, externalDate):
  def ret(entry: ExternalFullInfo)->ExternalInfo:
    if entry.revision is not None:
      return entry
    else:
      if entry.fullUrl.startswith(rootRepository+"/"):
        revision=getCommitBefore(entry.fullUrl, internalRevision).revision
      else:
        revision=getCommitBefore(entry.fullUrl, "{%s}"%(externalDate,)).revision
      return ExternalInfo(entry.name, entry.url, revision)
  return ret

def checkout(path, url, *, revision: str|CommitInfo|None=None, ignoreExternal=False):
  if isinstance(revision, CommitInfo):
    revision=revision.revision
  cmdLine=['checkout', url, path]
  if revision is not None:
    cmdLine.extend(('-r', f'{revision}'))
  if ignoreExternal:
    cmdLine.append('--ignore-externals')
  result=executeSvn(*cmdLine)

def export(path, url, *, revision: str|CommitInfo|None=None, ignoreExternal=False):
  if isinstance(revision, CommitInfo):
    revision=revision.revision
  cmdLine=['export', url, path]
  if revision is not None:
    cmdLine.extend(('-r', f'{revision}'))
  if ignoreExternal:
    cmdLine.append('--ignore-externals')
  result=executeSvn(*cmdLine)

def add(path):
  cmdLine=['add', path]
  result=executeSvn(*cmdLine)

def update(path):
  result=executeSvn('update', path)

def copy(src: str, dst: str, *, message: str):
  executeSvn('copy', src, dst, "-m", message)

def dateTimeFromString(s):
  return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

def toLocalDateTime(utcDateTime):
  now_timestamp = time.time()
  offset = datetime.datetime.fromtimestamp(now_timestamp) - datetime.datetime.utcfromtimestamp(now_timestamp)
  return utcDateTime + offset

def localTimeString(isoutc):
  datetime=dateTimeFromString(isoutc)
  local=toLocalDateTime(datetime)
  return local.strftime("%d/%m/%Y %H:%M:%S")
