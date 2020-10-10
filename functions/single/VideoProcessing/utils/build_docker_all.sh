
#!/bin/bash
set -e -u

docker login -u nivekiba -p nivekiba123

(
    cd runtime_moviepy
    docker build -t python3action:moviepy .
    docker tag python3action:moviepy nivekiba/python3action:moviepy
    docker push nivekiba/python3action:moviepy
)
