if [ "$1" == "" ]
then
    echo "Usage: " $0 "<bloom type>"
    exit
fi


sed -i -r -e "s/#define BLOOM [0-2]+/#define BLOOM $1/" test_meme.cc

