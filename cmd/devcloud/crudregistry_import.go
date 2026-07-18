// SPDX-License-Identifier: Apache-2.0

package main

// Blank import runs the generated CRUD registry's init(), registering every
// service's CRUD-shaped operations with the generic fallback engine
// (internal/shared/crud). Kept separate from the generated imports.go.
import _ "github.com/skyoo2003/devcloud/internal/generated/crudregistry"
