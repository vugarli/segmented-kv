package store

// herecy

func Unwrap(rw RWStore) *store {
	if rw == nil {
		return nil
	}
	return rw.(*store)
}

func UnwrapR(rw RStore) *store {
	if rw == nil {
		return nil
	}
	return rw.(*store)
}

func OpenTest(directory string, syncOnPut bool) (*store, error) {
	store, err := Open(directory, syncOnPut)
	return Unwrap(store), err
}

func OpenReadOnlyTest(directory string) (*store, error) {
	store, err := OpenReadOnly(directory)
	return UnwrapR(store), err
}
