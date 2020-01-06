package omnik

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// Adjecency indicates that something can accept adjecency information.
type Adjecency interface{}

// Before indicates that something should come before something else.
type Before struct {
	Adjecency
}

// After indicates that something should come after something else.
type After struct {
	Adjecency
}
