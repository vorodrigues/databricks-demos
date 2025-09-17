import logging
import os
import streamlit as st
from model_serving_utils import (
    endpoint_supports_feedback, 
    query_endpoint, 
    query_endpoint_stream, 
    _get_endpoint_task_type,
)
from threading_utils import NextThread
from collections import OrderedDict
from messages import UserMessage, AssistantResponse, render_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVING_ENDPOINT = os.getenv('SERVING_ENDPOINT')
assert SERVING_ENDPOINT, \
    ("Unable to determine serving endpoint to use for chatbot app. If developing locally, "
     "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
     "deploying to a Databricks app, include a serving endpoint resource named "
     "'serving_endpoint' with CAN_QUERY permissions, as described in "
     "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app")

ENDPOINT_SUPPORTS_FEEDBACK = endpoint_supports_feedback(SERVING_ENDPOINT)



# --- Init state ---
if "history" not in st.session_state:
    st.session_state.history = []

st.title("🧱 Chatbot App")
st.write(f"A basic chatbot using your own serving endpoint.")
st.write(f"Endpoint name: `{SERVING_ENDPOINT}`")



# --- Render chat history ---
for i, element in enumerate(st.session_state.history):
    element.render(i)



def query_endpoint_and_render(task_type, input_messages):
    """Handle streaming response based on task type."""
    if task_type == "agent/v1/responses":
        return query_responses_endpoint_and_render(input_messages)
    elif task_type == "agent/v2/chat":
        return query_chat_agent_endpoint_and_render(input_messages)
    else:  # chat/completions
        return query_chat_completions_endpoint_and_render(input_messages)


def query_chat_completions_endpoint_and_render(input_messages):
    """Handle ChatCompletions streaming format."""
    with st.chat_message("assistant"):
        st.write_stream("Pensando...")
        r = query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        )
        st.write_stream(r)
        return AssistantResponse(
            messages=[{"role": "assistant", "content":"FIM"}],
            request_id=1
        )


def query_chat_agent_endpoint_and_render(input_messages):
    """Handle ChatAgent streaming format."""
    from mlflow.types.agent import ChatAgentChunk
    
    with st.chat_message("assistant"):
        st.write_stream("Pensando...")
        r = query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        )
        st.write_stream(r)
        return AssistantResponse(
            messages=[{"role": "assistant", "content":"FIM"}],
            request_id=1
        )





def process_response(results):
  last_is_tool = False
  while True:
    try:
      thread = NextThread(target=next, arg=results)
      thread.start()
      thread.join()
      result = thread.result
      if result["type"] == "response.output_text.delta":
          last_is_tool = False
          yield result["delta"]
      elif (result["type"] == "response.output_item.done") and (result["item"]["type"] == "function_call") and (last_is_tool == False):
          last_is_tool = True
          yield "\n\nConsultando informações...\n\n"
    except StopIteration:
      break
    except TimeoutError:
      yield "\n\nA consulta demorou mais que o tempo máximo! Tente novamente em breve..."
      break


def query_responses_endpoint_and_render(input_messages):
    """Handle ResponsesAgent streaming format using MLflow types."""
    
    with st.chat_message("assistant"):
        
        r = query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        )
        p = process_response(r)
        c = st.write_stream(p)
    
    return AssistantResponse(
        messages=[{"role": "assistant", "content": c}],
        request_id=1
    )





# --- Chat input (must run BEFORE rendering messages) ---
prompt = st.chat_input("Faça uma pergunta...")
if prompt:
    # Get the task type for this endpoint
    task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
    
    # Add user message to chat history
    user_msg = UserMessage(content=prompt)
    st.session_state.history.append(user_msg)
    user_msg.render(len(st.session_state.history) - 1)

    # Convert history to standard chat message format for the query methods
    input_messages = [msg for elem in st.session_state.history for msg in elem.to_input_messages()]
    
    # Handle the response using the appropriate handler
    assistant_response = query_endpoint_and_render(task_type, input_messages)
    
    # Add assistant response to history
    st.session_state.history.append(assistant_response)