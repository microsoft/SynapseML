import getpass
import os
import shutil
import glob

current_username = getpass.getuser()

root_dir = f"/home/{current_username}/.ivy2/local/com.microsoft.azure/"


def find_second_level_folder(root):
    # Walk through the root directory
    for foldername, subfolders, filenames in os.walk(root):
        # Check if the current folder is a second-level folder by comparing its depth to the root's depth
        if foldername.count(os.path.sep) == root.count(os.path.sep) + 1:
            # Return the name of the second-level folder
            return os.path.basename(foldername)
    # Return None if no such folder is found
    return None


version = find_second_level_folder(root_dir)


def flatten_dir(top_dir):
    # Collect directories to delete
    directories_to_delete = []

    # Walk through all subdirectories
    for foldername, subfolders, filenames in os.walk(top_dir, topdown=False):
        # If we are not in the top-level directory, move files to the top-level directory
        if foldername != top_dir:
            for filename in filenames:
                source = os.path.join(foldername, filename)
                destination = os.path.join(top_dir, filename)

                # Check if a file with the same name already exists in the top-level directory
                if os.path.exists(destination):
                    base, ext = os.path.splitext(filename)
                    counter = 1
                    new_destination = os.path.join(top_dir, f"{base}_{counter}{ext}")

                    # Find a new destination path that does not exist yet
                    while os.path.exists(new_destination):
                        counter += 1
                        new_destination = os.path.join(
                            top_dir, f"{base}_{counter}{ext}"
                        )

                    destination = new_destination

                # Move file
                shutil.move(source, destination)
                print(f"Moved: {source} to {destination}")

            # Add the foldername to the list of directories to delete
            directories_to_delete.append(foldername)

    # Delete the old subdirectories
    for directory in directories_to_delete:
        os.rmdir(directory)
        print(f"Deleted: {directory}")


for top_dir in os.listdir(root_dir):
    path_to_jars = os.path.join(root_dir, top_dir)
    flatten_dir(path_to_jars)

    for file in os.listdir(path_to_jars):
        if "_2.13" in file and version not in file:
            old_file_path = os.path.join(path_to_jars, file)
            name_parts = file.split("_2.13")
            if name_parts[1].startswith(".") or name_parts[1].startswith("-"):
                sep_char = ""
            else:
                sep_char = "-"
            new_file = f"{name_parts[0]}_2.13-{version}{sep_char}{name_parts[1]}"
            new_file_path = os.path.join(path_to_jars, new_file)
            shutil.move(old_file_path, new_file_path)
