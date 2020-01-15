from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import TextOperationStatusCodes
from azure.cognitiveservices.vision.computervision.models import TextRecognitionMode
from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
from msrest.authentication import CognitiveServicesCredentials

from array import array
import os
from PIL import Image
import sys
import time

# Add your Computer Vision subscription key to your environment variables.
if 'COMPUTER_VISION_SUBSCRIPTION_KEY' in os.environ:
    subscription_key = os.environ['COMPUTER_VISION_SUBSCRIPTION_KEY']
else:
    print("\nSet the COMPUTER_VISION_SUBSCRIPTION_KEY environment variable.\n**Restart your shell or IDE for changes to take effect.**")
    sys.exit()
# Add your Computer Vision endpoint to your environment variables.
if 'COMPUTER_VISION_ENDPOINT' in os.environ:
    endpoint = os.environ['COMPUTER_VISION_ENDPOINT']
else:
    print("\nSet the COMPUTER_VISION_ENDPOINT environment variable.\n**Restart your shell or IDE for changes to take effect.**")
    sys.exit()

computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))
remote_image_url = "https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/landmark.jpg"

def readFile(verbose=False):
	'''
	Batch Read File, recognize printed text - remote
	This example will extract printed text in an image, then print results, line by line.
	This API call can also recognize handwriting (not shown).
	'''
	if verbose:
		print("===== Batch Read File - remote =====")
	last_time = time.time()
	# Get an image with printed text
	remote_image_printed_text_url = "https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/printed_text.jpg"
	# Call API with URL and raw response (allows you to get the operation location)
	recognize_printed_results = computervision_client.batch_read_file(remote_image_printed_text_url,  raw=True)

	# Get the operation location (URL with an ID at the end) from the response
	operation_location_remote = recognize_printed_results.headers["Operation-Location"]
	# Grab the ID from the URL
	operation_id = operation_location_remote.split("/")[-1]

	# Call the "GET" API and wait for it to retrieve the results 
	while True:
	    get_printed_text_results = computervision_client.get_read_operation_result(operation_id)
	    if get_printed_text_results.status not in ['NotStarted', 'Running']:
	        break
	    time.sleep(1)

	if verbose:
		# Print the detected text, line by line
		if get_printed_text_results.status == TextOperationStatusCodes.succeeded:
		    for text_result in get_printed_text_results.recognition_results:
		        for line in text_result.lines:
		            print(line.text)
		            print(line.bounding_box)

	runtime = time.time() - last_time

	# Call API with URL and raw response (allows you to get the operation location)
	recognize_printed_results = computervision_client.batch_read_file(remote_image_printed_text_url,  raw=True)

	print("Runtime: %6f" % runtime)
	return runtime

if __name__ == "__main__":
	readFile()