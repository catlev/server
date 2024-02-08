module github.com/catlev/server

go 1.21

require github.com/catlev/pkg v0.0.0-20240203114605-32d156d424f0

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/catlev/pkg => ../pkg

require (
	github.com/stretchr/testify v1.8.4
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
)
