#!bin/bash

ed=C

# Fetch the most recent version
VERSION=$(wget "https://www.jetbrains.com/intellij-repository/releases" -qO- | grep -P -o -m 1 "(?<=https://www.jetbrains.com/intellij-repository/releases/com/jetbrains/intellij/idea/BUILD/)[^/]+(?=/)")

# Prepend base URL for download
URL="https://download.jetbrains.com/idea/ideaI$ed-$VERSION.tar.gz"

echo $URL

# Truncate filename
FILE=$(basename ${URL})

# Set download directory
DEST=~/Downloads/$FILE

echo "Downloading idea-I$ed-$VERSION to $DEST..."
mkdir ~/Downloads/

# Download binary
wget -cO ${DEST} ${URL} --read-timeout=5 --tries=0

echo "Download complete!"

# Set directory name
DIR="/opt/idea-I$ed-$VERSION"

echo "Installing to $DIR"

# Untar file
if mkdir ${DIR}; then
    tar -xzf ${DEST} -C ${DIR} --strip-components=1
fi

# Grab executable folder
BIN="$DIR/bin"

# Add permissions to install directory
chmod -R +rwx ${DIR}

# Create symlink entry
ln -s ${BIN}/idea.sh /usr/local/bin/idea

echo "Done."  