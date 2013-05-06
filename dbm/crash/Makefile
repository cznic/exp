all:
	go fmt
	go test -i
	go test
	go build
	go vet
	make todo

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alnum:]] *.go || true
	@grep -n TODO *.go || true
	@grep -n BUG *.go || true

clean:
	rm -f *~ wal test.db

nuke: clean
	rm -f crash
