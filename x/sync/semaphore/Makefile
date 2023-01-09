sources: semaphore.go semaphore_test.go
%.go:
	curl https://raw.githubusercontent.com/golang/sync/master/semaphore/$@ > $@

.PHONE: sources *.go
