package scenarios

import "embed"

// FS contains all .fountain scenario scripts embedded at build time.
// Files are accessible as "<name>.fountain".
//
//go:embed *.fountain
var FS embed.FS
