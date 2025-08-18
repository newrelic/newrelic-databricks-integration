package databricks

import "context"

type MockIterator[T any] struct {
	HasNextFunc func(ctx context.Context) bool
	NextFunc    func(ctx context.Context) (T, error)
}

func (m *MockIterator[T]) HasNext(ctx context.Context) bool {
	return m.HasNextFunc(ctx)
}

func (m *MockIterator[T]) Next(ctx context.Context) (T, error) {
	return m.NextFunc(ctx)
}
