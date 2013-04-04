all:
	go fmt
	go test -i
	go test -timeout 1h
	go build
	go vet
	make todo

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alnum:]] *.go || true
	@grep -n TODO *.go || true
	@grep -n BUG *.go || true
	@grep -n println *.go || true

clean:
	rm -f *~ cov cov.html bad-dump good-dump lldb.test

gocov:
	gocov test $(COV) | gocov-html > cov.html
