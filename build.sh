docker build --platform linux/amd64 -t "cf-xlsx-reader:latest" \
            -t "us-central1-docker.pkg.dev/valid-bedrock-455402-***d6/cf-xlsx-reader/cf-xlsx-reader:latest" \
            -f ./Dockerfile \
            .
docker push -q "us-central1-docker.pkg.dev/valid-bedrock-455402-***d6/cf-xlsx-reader/cf-xlsx-reader:latest"