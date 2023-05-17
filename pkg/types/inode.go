package types

import (
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type InodeInfo struct {
	Attributes fuseops.InodeAttributes

	// File or directory?
	Dir bool

	// For directories, Children.
	Children []fuseutil.Dirent
}
