import numpy as np
import gradio as gr


# Cria um cliente do Vector Search
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
index = vsc.get_index('one-env-shared-endpoint-9', 'vr_demo.fashion_recommender.images_vs_index')


# Define a função para redimensionar as imagens
from PIL import Image
import io
def resize_image(image, width, height):
    resized_image = image.resize((width, height))
    byte_array = io.BytesIO()
    resized_image.save(byte_array, format='PNG')
    return byte_array.getvalue().decode('utf-8')


# Define a função executada após a ação do usuário
def get_similar_img(input_img: Image.Image) -> Image.Image:

  # Prepara a imagem fornecida pelo usuário
  img_str = resize_image(input_img, 128, 128)

  # Busca uma imagem similar
  results = index.similarity_search(
    query_text=img_str,
    columns=["content"],
    num_results=1
  )

  # Prepara a imagem recurepada
  content = results['result']['data_array'][0][0]
  output_img = Image.open(io.BytesIO(content.encode('utf-8')))

  return output_img


demo = gr.Interface(get_similar_img, gr.Image(), "image")
if __name__ == "__main__":
    demo.launch()