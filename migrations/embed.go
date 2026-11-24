// Package migrations embeds versioned SQL schema migrations.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
