package fusefs

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dashjay/tgzfs/pkg/types"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type Tgz struct {
	fuseutil.NotImplementedFileSystem
	opener      func() (*os.File, error)
	inodeTable  map[fuseops.InodeID]*types.InodeInfo
	direntTable map[fuseops.InodeID]string
}

func NewGzipFs(tgzFile string) (fuse.Server, error) {
	t := &Tgz{
		opener: func() (*os.File, error) {
			fd, err := os.Open(tgzFile)
			return fd, err
		},
		inodeTable:  make(map[fuseops.InodeID]*types.InodeInfo),
		direntTable: make(map[fuseops.InodeID]string),
	}

	return fuseutil.NewFileSystemServer(t), t.buildInodeTable()
}

func (t *Tgz) buildInodeTable() error {
	fd, err := t.opener()
	if err != nil {
		return err
	}
	defer fd.Close()
	gr, err := gzip.NewReader(fd)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gr)

	type entry struct {
		filePath string
		isDir    bool
		size     int64
	}
	var temp []entry
	for {
		th, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		temp = append(temp, entry{filePath: filepath.Clean(th.Name), isDir: th.FileInfo().IsDir(), size: th.FileInfo().Size()})
	}
	sort.Slice(temp, func(i, j int) bool {
		return strings.Compare(temp[i].filePath, temp[j].filePath) <= 0
	})

	t.inodeTable[fuseops.RootInodeID] = &types.InodeInfo{
		Attributes: fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0555 | fs.ModeDir,
		},
		Dir:      true,
		Children: make([]fuseutil.Dirent, 0),
	}
	nextFuseInodeID := fuseops.InodeID(fuseops.RootInodeID + 1)

	var findInodeInfo func(inodeId fuseops.InodeID) *types.InodeInfo
	var buildHelper func(inodeId fuseops.InodeID, ent *entry)

	findInodeInfo = func(inodeId fuseops.InodeID) *types.InodeInfo {
		ii, ok := t.inodeTable[inodeId]
		if ok == false {
			panic("no such inodeId " + strconv.Itoa(int(inodeId)))
		}
		return ii
	}

	getNextInodeID := func() fuseops.InodeID {
		next := nextFuseInodeID
		nextFuseInodeID++
		return next
	}

	buildHelper = func(inodeId fuseops.InodeID, ent *entry) {
		ii := findInodeInfo(inodeId)
		slashIdx := strings.Index(ent.filePath, "/")
		if slashIdx == -1 {
			nextInodeID := getNextInodeID()
			newInode := &types.InodeInfo{}
			offset := fuseops.DirOffset(len(ii.Children) + 1)
			newDirent := fuseutil.Dirent{
				Offset: offset,
				Inode:  nextInodeID,
				Name:   ent.filePath,
			}
			newInode.Dir = ent.isDir
			if ent.isDir {
				newInode.Attributes.Nlink = 1
				newInode.Attributes.Mode = 0555 | fs.ModeDir
				newDirent.Type = fuseutil.DT_Directory
			} else {
				log.Printf("")
				newInode.Attributes.Nlink = 1
				newInode.Attributes.Size = uint64(ent.size)
				newInode.Attributes.Mode = 0444
				newDirent.Type = fuseutil.DT_File
			}
			t.inodeTable[nextInodeID] = newInode
			ii.Children = append(ii.Children, newDirent)
		} else {
			prefix := ent.filePath[:slashIdx]
			var found bool
			var buildInodeID fuseops.InodeID
			for i := range ii.Children {
				if ii.Children[i].Name == prefix {
					found = true
					buildInodeID = ii.Children[i].Inode
				}
			}
			if found == false {
				nextInodeID := getNextInodeID()
				t.inodeTable[nextInodeID] = &types.InodeInfo{
					Attributes: fuseops.InodeAttributes{
						Mode: 0555 | fs.ModeDir,
					},
					Dir:      true,
					Children: make([]fuseutil.Dirent, 0),
				}
				offset := fuseops.DirOffset(len(ii.Children) + 1)
				ii.Children = append(ii.Children, fuseutil.Dirent{
					Offset: offset,
					Inode:  nextInodeID,
					Name:   prefix,
					Type:   fuseutil.DT_Directory,
				})
				buildInodeID = nextInodeID
			}
			buildHelper(buildInodeID, &entry{
				filePath: ent.filePath[slashIdx+1:],
				isDir:    ent.isDir,
				size:     ent.size,
			})
		}
	}

	var buildDentry func(prefix string, inodeId fuseops.InodeID)
	buildDentry = func(prefix string, inodeId fuseops.InodeID) {
		ii := t.inodeTable[inodeId]
		for i := range ii.Children {
			if ii.Children[i].Type == fuseutil.DT_Directory {
				buildDentry(filepath.Join(prefix, ii.Children[i].Name), ii.Children[i].Inode)
			} else {
				t.direntTable[ii.Children[i].Inode] = filepath.Join(prefix, ii.Children[i].Name)
			}
		}
	}
	for i := range temp {
		buildHelper(fuseops.RootInodeID, &temp[i])
	}
	buildDentry("", fuseops.RootInodeID)
	return nil
}

func (t *Tgz) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (t *Tgz) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	// Allow opening any directory.
	return nil
}

func (t *Tgz) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	// Find the info for this inode.
	info, ok := t.inodeTable[op.Inode]
	if !ok {
		return fuse.ENOENT
	}

	if !info.Dir {
		return fuse.EIO
	}

	entries := info.Children

	// Grab the range of interest.
	if op.Offset > fuseops.DirOffset(len(entries)) {
		return fuse.EIO
	}

	entries = entries[op.Offset:]

	// Resume at the specified offset into the array.
	for _, e := range entries {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], e)
		if n == 0 {
			break
		}

		op.BytesRead += n
	}
	return nil
}

func (t *Tgz) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	info, ok := t.inodeTable[op.Inode]
	if !ok {
		return fuse.ENOENT
	}

	// Copy over its Attributes.
	op.Attributes = info.Attributes
	log.Printf("GetInodeAttributes(%d), attr: %#v", op.Inode, info.Attributes)
	return nil
}

func findChildInode(name string, in []fuseutil.Dirent) (fuseops.InodeID, error) {
	for i := range in {
		if in[i].Name == name {
			return in[i].Inode, nil
		}
	}
	return 0, fuse.ENOENT
}

func (t *Tgz) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {

	// Find the info for the parent.
	parentInfo, ok := t.inodeTable[op.Parent]
	if !ok {
		return fuse.ENOENT
	}

	// Find the child within the parent.
	childInode, err := findChildInode(op.Name, parentInfo.Children)
	if err != nil {
		return err
	}

	// Copy over information.
	op.Entry.Child = childInode
	op.Entry.Attributes = t.inodeTable[childInode].Attributes

	return nil
}

func (t *Tgz) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	// Allow opening any file.
	return nil
}

func (t *Tgz) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	filename, ok := t.direntTable[op.Inode]
	if !ok {
		return fuse.ENOENT
	}
	fd, err := t.opener()
	if err != nil {
		return fuse.EIO
	}
	defer fd.Close()
	gr, err := gzip.NewReader(fd)
	if err != nil {
		return fuse.EIO
	}
	tr := tar.NewReader(gr)
	for {
		th, err := tr.Next()
		if err != nil {
			return fuse.EIO
		}
		if filepath.Clean(th.Name) == filename {
			_, _ = tr.Read(make([]byte, op.Offset))
			op.BytesRead, err = tr.Read(op.Dst)
			if err == io.EOF {
				return nil
			}
		}
	}
}
