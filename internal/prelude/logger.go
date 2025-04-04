package prelude

import "go.uber.org/zap"

func InitLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	return logger
}