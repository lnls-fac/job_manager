# Destination directory
DEST_DIR = /usr/local/bin

SCRIPTS = $(shell ls *.py)

install: $(SCRIPTS)
	cp -f $(SCRIPTS) $(DEST_DIR); cd $(DEST_DIR); chmod a+x $(SCRIPTS) 
uninstall:
	cd $(DEST_DIR); rm -rf $(SCRIPTS); cd -

develop: $(SCRIPTS) uninstall
	ln -srf $(SCRIPTS) $(DEST_DIR)
