package spark

// MockSparkMetricDecorator is a mock implementation of SparkMetricDecorator for testing
type MockSparkMetricDecorator struct {
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
	DecorateMetricFunc   func(attrs map[string]interface{})
}

func (m *MockSparkMetricDecorator) DecorateExecutor(
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

func (m *MockSparkMetricDecorator) DecorateJob(
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

func (m *MockSparkMetricDecorator) DecorateStage(
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

func (m *MockSparkMetricDecorator) DecorateTask(
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

func (m *MockSparkMetricDecorator) DecorateRDD(
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

func (m *MockSparkMetricDecorator) DecorateMetric(
	attrs map[string]interface{},
) {
	if m.DecorateMetricFunc != nil {
		m.DecorateMetricFunc(attrs)

		return
	}

	// Default implementation adds common attributes
	m.decorate(attrs)
}

func (m *MockSparkMetricDecorator) decorate(attrs map[string]interface{}) {
	// Add Databricks workspace attributes
	attrs["databricksWorkspaceId"] = int64(12345)
	attrs["databricksWorkspaceName"] = "foo.fakedomain.local"
	attrs["databricksWorkspaceUrl"] = "https://foo.fakedomain.local"
}
