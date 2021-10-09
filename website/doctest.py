import os

def main():
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "website", "docs", "documentation")
    os.chdir(folder)
    os.system('pytest --codeblocks')

if __name__ == '__main__':
    main()
