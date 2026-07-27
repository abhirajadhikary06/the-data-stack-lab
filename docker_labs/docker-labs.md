### Get into DockerHub
- `docker login` - Login to DockerHub (registry)
- `docker tag <image-name> <dockerhub-username>/<image-name>:latest` - tag the image with your Docker Hub username
- `docker push <dockerhub-usernaame>/<image-name>:latest`

---

### Pulling an Image from Docker Registry
- `docker pull hello-world` - pull command
- `docker pull alpine:latest` - pull command (very light weight linux image)
- `docker run -it alpine sh` - run the alpine image

---

### Create a Docker Image
- We build a basic docker image of the `Dockerfile.basic`
- `docker build -f Dockerfile.basic -t basic-alpine .` - build the docker image on our Dockerfile.basic and create image on basic-app.py
- `docker run basic-alpine` - run the image that we built

---

### For practise we create another `Dockerfile.ubuntu`
- We build a basic docker image of the `Dockerfile.ubuntubasic`
- `docker build -f Dockerfile.basic -t basic-ubuntu .` - build the docker image on our Dockerfile.basic and create image on basic-app.py
- `docker run basic-ubuntu` - run the image that we built

---

### Deploying Flask App to Docker
- We create a rate-limiting application using `flask-limiter` python library
- Create a `Dockerfile.flask` in that we EXPOSE port 5000 for flask and CMD is set to run the flask application
- `docker build -f Dockerfile.basic -t flask-app .` - Build the flask-app image
- `docker run flask-app` - Run the flask-app

---

### Building a MultiStage Dockerfile for a simple golang api application
- Here we build the Dockerfile image in two stage one is Build Stage and another is Runtime Stage
- In the Build part we keep the build dependencies it includes compilers, pip, npm, or other build tools
- In the Stage part we keep only runtime libraries are copied in, fewer package, lower vulnerability, faster download rate
- Benefits - image size reduction, security, clean separation, different package for different stages (build, test, runtime)
- We use `golang:1.22` in our Build stage as `builder` and COPY --from=build that in `alpine:latest` in Runtime stage

---

### Building a MultiStage Distroless Dockerfile on `basic-app.py` python file
- It is a minimal docker image with application binary, runtime dependency and configuration metadata. Distroless images excludes shells (bash, sh), package manager (apt, apk), debugging tools (curl, wget), unnecessary linux OS
- Here for the Build image we use `python:3.11-slim` and for distroless image `python:3.11-slim`. The size of multistage-distroless is way less than general multistage image

---

## Bridge Network

```
Host Machine
┌─────────────────────────────────────┐
│                                     │
│  [container A] ──┐                  │
│                  ├── bridge0 ──▶ internet
│  [container B] ──┘                  │
│                                     │
│  [container C] ── bridge1 (isolated)│
└─────────────────────────────────────┘
```

### Creating a default Bridge Network
- `docker network ls` - check the list of networks

```
NETWORK ID     NAME      DRIVER    SCOPE
065fee22a390   bridge    bridge    local  <---- default bridge network
e58ef895c3a4   host      host      local
9e0e96c85da7   none      null      local
```

- `docker network inspect bridge` - Inspect the bridge network like Subnet IP, Gateway
- `docker run -d --name c1 nginx` - Run first docker container of nginx
- `docker run -d --name c2 nginx` - Run second docker container of nginx
- `docker inspect c1 --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"` - Check the IP on which container `c1` of nginx is running
- `docker exec c2 apt-get update -q && apt-get install -y iputils-ping` - To install ping command in c2 container
- `docker exec c2 ping <ip-of-c1> -c 3` - Ping the IP of c1 container from c2, 3 times

---

### Creating a custom Bridge Network
- `docker network create mybridge` - Create a custom bridge network and name it mybridge
- `docker run -d --name app1 --network mybridge nginx` - Create first container `app1` of nginx on the `mybridge` (custom bridge network)
- `docker run -d --name app2 --network mybridge nginx` - Create second container `app2` of nginx on the `mybridge` (custom bridge network)
- `docker exec app2 apt-get update -q && apt-get install -y iputils-ping` - To install ping command in app2 container
- `docker exec app2 ping app1 -c 3` - Ping `app1` from `app2`, this will get executed successfully without any packet drop because they are on same network
- `docker exec c2 ping <ip-of-app1> -c 3` - Ping `app1` from `c2`, this will show 100% packet drop because `app1` is in an isolated custom network

---

## Docker Bind Mounts & Volumes

Containers are ephemeral i.e when we remove a container all the data inside the container is gone. So to persist the data we came up with concepts of Bind Mount and Volume.

```
┌─────────────────────────────────────────────┐
│              Your Host Machine              │
│                                             │
│  /your/folder  ←── Bind Mount (you control) │
│                                             │
│  Docker Area   ←── Volume (Docker controls) │
└─────────────────────────────────────────────┘
```

### Difference between Bind Mount and Volume
- **Bind Mount** - mounts data from a specified path on the host machine (host acts as the storage); the path is specified by the user; it is not portable across machines; bind mount is preferred during development stage
- **Volume** - data is stored in docker managed area (`/var/lib/docker/volumes/`); the storage path is not human controllable it is auto created by Docker; volumes are portable across machines; volumes are generally preferred for production stage (db, logs, cache)

---

### Bind Mounts
- `echo "Hello from host" > "C:\Abhiraj\Data Engineering\docker_labs\test.txt"` - We create a `test.txt` file with some data
- `docker run -it --rm -v "C:\Abhiraj\Data Engineering\docker_labs:/data" ubuntu bash` - Mount the folder into the container
- `cat /data/test.txt && echo "Written from Container" && exit >> /data/test.txt` - This command opens the mounted `test.txt` file in `/data` directory and writes some content in it i.e "Written from Container"
- `type "C:\Abhiraj\Data Engineering\docker_labs\test.txt"` - When we check type in the host machine we can see both the written content:

```bash
Hello from host
Written from Container
```

- `docker run -d -p 5000:5000 -v "C:\Abhiraj\Data Engineering\docker_labs:/app" flask-app` - We mount the `flask-app` which is on the host to the container, in this way when changes are made in host it is updated to container in real time, `CMD ["/app/venv/bin/python3", "flask-app.py"]`

---

### Volume

- `docker volume create mydata` - Command to create a docker volume
- `docker volume ls` - Check all the present volumes
- `docker volume inspect mydata` - It tells all the metadata about the `mydata` docker volume, it is mounted at the location `/var/lib/docker/volumes/mydata/_data`
- `docker run -it --rm -v mydata:/data ubuntu bash` - This command creates a container with an interactive shell as `bash` and mounts the volume `mydata` to the `/data` directory, that uses the `ubuntu` image
- `echo "persisted data" > /data/volume-data.txt` - This writes data to volume-data.txt
- `docker run -d --name writer -v mydata:/data ubuntu sh -c "while true; do date >> /data/log.txt; sleep 2; done"` - We initialize a writer to write data to the `log.txt` file with a condition of creating log every 2 seconds
- `docker run -it --rm -v mydata:/data ubuntu bash` - With this command we enter the interactive bash shell
- `tail -f /data/log.txt` - This command prints the data generated in the write command in the ubuntu read container that reads from the log.txt file

---

**Another Example:**
- `docker rm -f writer` - We stop the previous writer of the ubuntu container
- `docker run -d --name writer -v mydata:/data ubuntu sh -c "echo 'My Persistent Data' >> /data/persistent.txt"` - We initialize a writer to write data to the `persistent.txt`
- `docker run -it --rm -v mydata:/data ubuntu bash` - With this command we enter the interactive bash shell
- `cat /data/persistent.txt` - Using this command we check the content inside persistent.txt

---

### Docker Compose
- Plain docker is good for managing single image, but when we need to run multiple images which are inturn inter related to each other through shared volumes (storage) or network then we use dockercompose.
- We create an `.yaml` file it references the Dockerfiles and in `.yaml` we write configuration of how to build, connect and run the services together.
```yaml
services: # in this we define all the sevices (images we want to start)
  app: # this is one service built up from scratch
    image: myapp:latest
    build:
      context: ./app # as this is built from scratch, this specifies the source
    ports:
      - "3000:3000" # the port on which this container will be running
    environment:
      APP_ENV: production
    volumes: # the volume that this container is assigned to
      - app_data:/usr/src/app
    networks: # the network that this container is assigned to
      - app_network
    depends_on: # the app container depends on the db container
      - db

  db: # this is one service using official postgres-docker image 
    image: postgres:latest
    environment: # set the secret credentials
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: app_db
    volumes: # the storage (volume) that the container is mounted to
      - db_data:/var/lib/postgresql/data
    networks: # the network that this container is assigned to 
      - app_network
    restart: always # on crash this automatically restarts (other opt: no, on-failure, unless-stopped)

networks: # the network configuration and the name of network
  app_network:
    driver: bridge

volumes: # the name of the storage/volume
  app_data:
  db_data:
```
- `docker compose up -d` - Create and start all containers defined in docker compose
- `docker compose up --build` - Start building the containers
- `docker compose down` - Stop and remove containers and network associated
- `docker compose stop` - Only stop running of the containers
- `docker compose restart` - Restart all the containers
- `docker compose ps` - List all containers and their status
- `docker compose logs` - Show logs from all services
- `docker compose rm` - Remove stopped containers
- `docker compose down --rmi all` - Remove all containers and built images
- `docker compose down --rmi all -v` - Remove all containers and built images along with the volumes