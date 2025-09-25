package spark

// MockSparkEventDecorator is a mock implementation of SparkEventDecorator for
// testing
type MockSparkEventDecorator struct {
	DecorateExecutorFunc func(
		sparkExecutor *SparkExecutor,
		attrs map[string]interface{},
	)
	DecorateJobFunc      func(
		sparkJob *SparkJob,
		attrs map[string]interface{},
	)
	DecorateStageFunc    func(
		sparkStage *SparkStage,
		attrs map[string]interface{},
	)
	DecorateTaskFunc     func(
		sparkStage *SparkStage,
		sparkTask *SparkTask,
		attrs map[string]interface{},
	)
	DecorateRDDFunc      func(
		sparkRDD *SparkRDD,
		attrs map[string]interface{},
	)
	DecorateEventFunc   func(attrs map[string]interface{})
}

func (m *MockSparkEventDecorator) DecorateExecutor(
	sparkExecutor *SparkExecutor,
	attrs map[string]interface{},
) {
	if m.DecorateExecutorFunc != nil {
		m.DecorateExecutorFunc(sparkExecutor, attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) DecorateJob(
	sparkJob *SparkJob,
	attrs map[string]interface{},
) {
	if m.DecorateJobFunc != nil {
		m.DecorateJobFunc(sparkJob, attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) DecorateStage(
	sparkStage *SparkStage,
	attrs map[string]interface{},
) {
	if m.DecorateStageFunc != nil {
		m.DecorateStageFunc(sparkStage, attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) DecorateTask(
	sparkStage *SparkStage,
	sparkTask *SparkTask,
	attrs map[string]interface{},
) {
	if m.DecorateTaskFunc != nil {
		m.DecorateTaskFunc(sparkStage, sparkTask, attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) DecorateRDD(
	rdd *SparkRDD,
	attrs map[string]interface{},
) {
	if m.DecorateRDDFunc != nil {
		m.DecorateRDDFunc(rdd, attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) DecorateEvent(
	attrs map[string]interface{},
) {
	if m.DecorateEventFunc != nil {
		m.DecorateEventFunc(attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkEventDecorator) decorate(attrs map[string]interface{}) {
	// Add Databricks workspace attributes
	attrs["databricksWorkspaceId"] = int64(12345)
	attrs["databricksWorkspaceName"] = "foo.fakedomain.local"
	attrs["databricksWorkspaceUrl"] = "https://foo.fakedomain.local"
}
