import gradio as gr
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from PIL import Image
import codecs
import io
import logging
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import os



# Ensure environment variable is set correctly
# assert os.getenv('SERVING_ENDPOINT'), "SERVING_ENDPOINT must be set in app.yaml."
endpoint_name = os.getenv('SERVING_ENDPOINT')
print(endpoint_name)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the Databricks Workspace Client
w = WorkspaceClient()



def add_bounding_boxes(image, object_name, all_coordinates, all_confidences):
  if len(all_coordinates) > 0:
    # Create a figure and axis
    fig, ax = plt.subplots(1)

    # Display the image
    ax.imshow(image)
    
    # Define a color map for different confidence levels
    colors = ['r', 'g', 'b', 'y', 'c', 'm']

    # Create Rectangle patches for each detection
    for coordinates, confidence in zip(all_coordinates, all_confidences):
      color_index = min(int(confidence * len(colors)), len(colors) - 1)
      color = colors[color_index]
      
      # Unpack coordinates
      x1, y1, x2, y2 = coordinates
      
      rect = patches.Rectangle((x1, y1), 
                                x2 - x1, 
                                y2 - y1, 
                                linewidth=2, edgecolor=color, facecolor='none')
      
      # Add the patch to the Axes
      ax.add_patch(rect)

      # Add label
      plt.text(x1, y1 - 10, 
                f"{object_name} ({confidence:.3f})", 
                color=color, fontsize=10, weight='bold')

    # Turn off axis
    plt.axis('off')
    
    # Set the title
    plt.title(f"Detected {object_name}s", fontsize=14, fontweight='bold')
    
    # Convert to Image object
    byte_array = io.BytesIO()
    plt.savefig(byte_array, format='png')
    return Image.open(byte_array)

  else:
    return image



def detect_objects(image: Image.Image) -> Image.Image:

  print(image)

  # Convert image to string
  byte_array = io.BytesIO()
  image.save(byte_array, format='PNG')
  img_str = codecs.decode(codecs.encode(byte_array.getvalue(), "base64"), 'utf-8')

  # Query the endpoint
  response = w.serving_endpoints.query(endpoint_name, inputs=[{"image": img_str}])
  result = response.predictions[0]

  # Add bounding boxes to the image
  img_out = add_bounding_boxes(image, "package", result['pkg_coordinates'], result['pkg_confidences'])
  
  return img_out



demo = gr.Interface(
  fn=detect_objects, 
  inputs=gr.Image(type='pil'), 
  outputs="image", 
  css="footer {visibility: hidden}",
  title="Delivery Confirmation",
  flagging_mode="never"
)

if __name__ == "__main__":
    demo.launch(favicon_path="img/favicon.ico", share=True)