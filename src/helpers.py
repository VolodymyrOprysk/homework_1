import os
import subprocess


def copy_csv_to_docker_container(local_path: str, container_name: str, container_dest_path: str):
    """Copy a local CSV file into a Docker container using `docker cp`."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"CSV file not found: {local_path}")

    try:
        subprocess.run(
            ['docker', 'cp', local_path,
                f'{container_name}:{container_dest_path}'],
            check=True
        )
        print(f"{local_path} CSV copied to container: {container_dest_path}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to copy file into Docker container: {e}")


def delete_csv_from_docker_container(container_name: str, container_file_path: str):
    """Delete a file from inside a Docker container using `docker exec`."""
    try:
        subprocess.run(
            ['docker', 'exec', container_name, 'rm', '-f', container_file_path],
            check=True
        )
        print(f"Temp CSV file deleted from container: {container_file_path}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to delete file from Docker container: {e}")
