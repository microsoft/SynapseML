from service import *


if __name__ == "__main__":
	service_type = input("Would you like to update a container or API service?\n\t(1) Container\n\t(2) API\n")
	while service_type.lower() not in ["1", "2", "container", "api"]:
		service_type = input("Sorry, I didn't get that. Please enter 1, 2, \"container\", or \"api\".")
	
	service = input("Which service would you like to update?\n\t(1) Text Analytics\n\t(2) Computer Vision\n")
	while service.lower() not in ["1", "2", "text", "vision"]:
		service = input("Sorry, I didn't get that. Please enter 1, 2, \"text\", or \"vision\".")
