# mongoarchivereader

Takes a mongo archive, created from mongodump with the `--archive` flag, and either
lists the contents of the archive or dumps the content of the archive to files.
The result is a dump directory which should contain the same content as if mongodump
was executed with the `--out` flag.

Attempting to somewhat follow with https://github.com/mongodb/mongo-tools/ so the
flags should work as expected for people familiar with the mongo-tools.

Wrote this to help debug a mongorestore problem by allowing the comparison of the
contents of an archive which was able to restore and the content of an archive that
was producing errors during the restore process. The `--list` option is also useful
for viewing the databases and collections contained within the archive without
needing an actual `mongod` instance to do a `mongorestore --dryRun` against.

```
$ ./mongoarchivereader --help
Usage:
  mongoarchivereader <options> 

Take an archive from mongodump and list or reconstruct it as a dump of bson and metadata.json files

general options:
      --help                    print usage
      --version                 print the tool version and exit
      --config=                 path to a configuration file

verbosity options:
  -v, --verbose=<level>         more detailed log output (include multiple times for more verbosity, e.g. -vvvvv, or specify a numeric value, e.g. --verbose=N)
      --quiet                   hide all log output

tool specific options:
  -o, --out=<directory-path>    output directory to place reconstructed archive (default: '<archive>.dump')
      --gzip                    supplied archive is gzipped
      --archive=<file-path>     archive to be processed
      --list                    list contents of archive instead of reconstructing it
```

## Example Usages

List content of archive:
```
$ ./mongoarchivereader --archive=/home/thaskell/mongodumparchive.gz --gzip --list
2021-02-04T11:45:06.615-0600	oplog.bson
2021-02-04T11:45:06.615-0600	db/new.bson
2021-02-04T11:45:06.615-0600	db/new.metadata.json
2021-02-04T11:45:06.615-0600	db/qm_canary.bson
2021-02-04T11:45:06.615-0600	db/qm_canary.metadata.json
2021-02-04T11:45:06.615-0600	admin/system.users.bson
2021-02-04T11:45:06.615-0600	admin/system.users.metadata.json
2021-02-04T11:45:06.615-0600	admin/system.roles.bson
2021-02-04T11:45:06.615-0600	admin/system.roles.metadata.json
2021-02-04T11:45:06.615-0600	admin/system.version.bson
2021-02-04T11:45:06.615-0600	admin/system.version.metadata.json
```

Reconstruct content of archive to default location:
```
$ ./mongoarchivereader --archive=/home/thaskell/mongodumparchive.gz --gzip
2021-02-04T11:48:15.845-0600	--out not specified, defaulting to `/home/thaskell/mongodumparchive.gz.dump`
2021-02-04T11:48:15.901-0600	finished restoring metadata for db.new
2021-02-04T11:48:15.903-0600	finished restoring db.new (1 document, 0 failures)
2021-02-04T11:48:15.903-0600	finished restoring metadata for db.qm_canary
2021-02-04T11:48:15.906-0600	finished restoring db.qm_canary (1 document, 0 failures)
2021-02-04T11:48:15.916-0600	finished restoring admin.system.version (2 documents, 0 failures)
2021-02-04T11:48:15.932-0600	finished restoring .oplog (1 document, 0 failures)
2021-02-04T11:48:15.934-0600	finished restoring admin.system.users (4 documents, 0 failures)
2021-02-04T11:48:15.944-0600	finished restoring admin.system.roles (1 document, 0 failures)
```

Output of reconstruction:
```
$ ls -1R /home/thaskell/mongodumparchive.gz.dump
/home/thaskell/mongodumparchive.gz.dump:
admin
db
oplog.bson

/home/thaskell/mongodumparchive.gz.dump/admin:
system.roles.bson
system.users.bson
system.version.bson

/home/thaskell/mongodumparchive.gz.dump/db:
new.bson
new.metadata.json
qm_canary.bson
qm_canary.metadata.json
```

bsondump can be used as expect to look at the content of the dumped bson files:
```
$ bsondump /home/thaskell/mongodumparchive.gz.dump/oplog.bson 2> /dev/null | jq .
{
  "ts": {
    "$timestamp": {
      "t": 1611768925,
      "i": 1
    }
  },
  "t": {
    "$numberLong": "1"
  },
  "h": {
    "$numberLong": "233768039837787906"
  },
  "v": 2,
  "op": "n",
  "ns": "",
  "wall": {
    "$date": "2021-01-27T17:35:25.936Z"
  },
  "o": {
    "msg": "periodic noop"
  }
}
```

## Install

```bash
$ go get github.com/thaskell/mongoarchivereader
```

## Build

Requires Go 1.16

```bash
$ git clone git@github.com:thaskell/mongoarchivereader.git
$ cd mongoarchivereader/cmd/mongo-archive-reader
$ go build
```
