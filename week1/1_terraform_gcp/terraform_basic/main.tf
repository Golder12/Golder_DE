#create a container
resource "docker_container" "nginx" {
    name = "nginx"
    image = docker_image.nginx-image.latest

    ports{
        internal = "80"
        external = "8080"
    }

}

#pulls the image
resource "docker_image" "nginx-image" {
    name = "nginx"
}