import os
import datetime
from databricks import sql
from databricks.sdk.core import Config
import gradio as gr
import pandas as pd

# Ensure environment variables are set correctly
assert os.getenv('JOB_ID'), "JOB_ID must be set in app.yaml."
JOB_ID = os.getenv('JOB_ID')
assert os.getenv('DATABRICKS_APP_PORT'), "DATABRICKS_APP_PORT not found."
DATABRICKS_APP_PORT = int(os.getenv('DATABRICKS_APP_PORT'))


from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

def run_job(exp_name: str, pct_train: float, n_exp: int, min_depth: int, max_depth: int, min_learning_rate: float, max_learning_rate: float):
  try:
    w.jobs.run_now(
      job_id=JOB_ID,
      job_parameters={
        'exp_name': exp_name,
        'pct_train': pct_train,
        'n_exp': n_exp,
        'min_depth': min_depth,
        'max_depth': max_depth,
        'min_learning_rate': min_learning_rate,
        'max_learning_rate': max_learning_rate
      }
    )
    gr.Info("AutoML experiment started!")
  except Exception as e:
    print(e)
    raise gr.Error("Failed to start AutoML job! "+str(e))



# display the data with Gradio
with gr.Blocks(title="AutoML App", css="footer {visibility: hidden}") as app:
    
    # Header
    with gr.Row(height=84):
        with gr.Column(scale=0, min_width=84):
            gr.HTML('<img src=favicon.ico>')
        with gr.Column(scale=1):
            gr.HTML('<div style="height: 64px; line-height: 64px"><font size="6"><b>AutoML App</b></font></div>')

    # Tabs
    with gr.Row():
        with gr.Tab("Travel Expert"):
            te_exp_name = gr.Textbox(value='Type your experiment name', label="Experiment name")
            te_pct_train = gr.Textbox(value=0.7, label="Train size")
            te_n_exp = gr.Textbox(value=10, label="Number of experiments")
            with gr.Accordion("Advanced", open=False):
                te_min_depth = gr.Textbox(value=1, label="Min. depth")
                te_max_depth = gr.Textbox(value=10, label="Max. depth")
                te_min_learning_rate = gr.Textbox(value=0.01, label="Min. learning rate")
                te_max_learning_rate = gr.Textbox(value=0.40, label="Max. learning rate")
            te_button = gr.Button("Run", )
        with gr.Tab("Segmentation"):
            gr.Textbox(value='Type your experiment name', label="Experiment name")
            gr.Textbox(value=0.7, label="Train size")
            gr.Textbox(value=10, label="Number of experiments")
        with gr.Tab("Route Optimization"):
            gr.Textbox(value='Type your experiment name', label="Experiment name")
            gr.Textbox(value=0.7, label="Train size")
            gr.Textbox(value=10, label="Number of experiments")

    # Event: click on run travel expert
    te_button.click(
        run_job, 
        inputs=[
            te_exp_name,
            te_pct_train,
            te_n_exp,
            te_min_depth,
            te_max_depth,
            te_min_learning_rate,
            te_max_learning_rate
        ]
    )

if __name__ == "__main__":
    app.launch(debug=True, favicon_path="img/favicon.ico", server_name='0.0.0.0', server_port=DATABRICKS_APP_PORT, share=True)