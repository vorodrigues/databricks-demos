import functools
import os
from typing import Any, Generator, Literal, Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    VectorSearchRetrieverTool
)
from databricks_langchain.genie import GenieAgent
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel
from databricks.sdk.credentials_provider import ModelServingUserCredentials


from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser


###################################################
## Create a RAG Agent
###################################################

# TODO fill in fields below

faq_index = {
    "index_name": "vr_demo.playground.faq_index",
    "description": "Use esta função para responder perguntas sobre prazos de entrega, pedidos de troca ou devolução, entre outras perguntas frequentes sobre o nosso marketplace.",
    "name": "faq_index",
    "columns": ["resposta"],
    "num_results": 1,
    "host": "https://e2-demo-field-eng.cloud.databricks.com"
}

def create_vs_tool(index_name, description, name, columns, num_results, host):
    # Use user authenticated client to initialize a vector search retrieval tool
    user_authenticated_client = WorkspaceClient(
        host=host,
        credentials_strategy=ModelServingUserCredentials()
    )
    vs_tool = VectorSearchRetrieverTool(
        index_name=index_name,
        description=description,
        tool_name=name,
        columns=columns,
        num_results=num_results,
        workspace_client=user_authenticated_client
    )
    return vs_tool

def create_rag_agent(index_name, description, name, columns, num_results, host):
    faq_tool = create_vs_tool(index_name, description, name, columns, num_results, host)

    # # Select our Databricks Foundation Model
    # llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct", temperature=0.1)

    # # Define our prompt
    # template = '''
    # Você é um assistente especialista e pode responder perguntas sobre prazos de entrega, pedidos de troca ou devolução, entre outras sobre o nosso marketplace.
    # Se a pergunta não for sobre um desses assuntos, educadamente responda que você não pode responder a este tipo de pergunta.
    # Use o contexto abaixo para responder às perguntas. Se o contexto não fornecer uma resposta satisfatória, apenas diga que você não sabe. Não tente inventar uma resposta.

    # Contexto: {context}

    # Pergunta: {question}

    # Resposta: '''

    # # Create a prompt template
    # prompt = PromptTemplate(
    #     template=template, 
    #     input_variables=[
    #         'context', 
    #         'question',
    #     ]
    # )

    # Define a function to join the retrieve documents
    def format_docs(docs):
        return "\n\n".join(doc.metadata['resposta'] for doc in docs)

    # Chain all steps together
    # return (
    #     {
    #         "context": faq_tool | format_docs,
    #         "question": RunnablePassthrough(),
    #     }
    #     | prompt
    #     | llm
    #     | StrOutputParser()
    # )

    return (
        faq_tool
        | format_docs
        | StrOutputParser()
    )

def rag_node(state, agent, name):
    result = agent.invoke(state["messages"][-1]["content"])
    return {
        "messages": [
            {
                "role": "assistant",
                "content": f"Contexto: {result}",
                "name": name,
            }
        ]
    }


###################################################
## Create a GenieAgent with access to a Genie Space
###################################################

# TODO add GENIE_SPACE_ID and a description for this space
# You can find the ID in the URL of the genie room /genie/rooms/<GENIE_SPACE_ID>

sales_genie = {
    "genie_space_id": "01f058479ad31fd0b21551c3ce350db9",
    "name": "SalesGenie",
    "description": "Use esta ferramenta para responder perguntas sobre vendas",
    "host": "https://e2-demo-field-eng.cloud.databricks.com"
}

log_genie = {
    "genie_space_id": "01f06eccbb7f1f6bae899688b4a95376",
    "name": "LogGenie",
    "description": "Use esta ferramenta para responder perguntas sobre logística e estoque",
    "host": "https://e2-demo-field-eng.cloud.databricks.com"
}

def create_genie_agent(genie_space_id, name, description, host):
    # Use user authenticated client to initialize a vector search retrieval tool
    user_authenticated_client = WorkspaceClient(
        host=host,
        credentials_strategy=ModelServingUserCredentials()
    )

    return GenieAgent(
        genie_space_id=genie_space_id,
        genie_agent_name=name,
        description=description,
        client=user_authenticated_client
    )


############################################
# Define your LLM endpoint and system prompt
############################################

# TODO: Replace with your model serving endpoint
# multi-agent Genie works best with claude 3.7 or gpt 4o models.
LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)


#############################
# Define the supervisor agent
#############################

# TODO update the max number of iterations between supervisor and worker nodes
# before returning to the user
MAX_ITERATIONS = 3

worker_descriptions = {
    faq_index["name"]: faq_index["description"],
    sales_genie["name"]: sales_genie["description"],
    log_genie["name"]: log_genie["description"]
}

formatted_descriptions = "\n".join(
    f"- {name}: {desc}" for name, desc in worker_descriptions.items()
)

system_prompt = f"Escolha entre encaminhar para um dos trabalhadores abaixo ou encerrar a conversa se uma resposta for fornecida. \n{formatted_descriptions}"
options = ["FINISH"] + list(worker_descriptions.keys())
FINISH = {"next_node": "FINISH"}

def supervisor_agent(state):
    count = state.get("iteration_count", 0) + 1
    if count > MAX_ITERATIONS:
        return FINISH
    
    class nextNode(BaseModel):
        next_node: Literal[tuple(options)]

    preprocessor = RunnableLambda(
        lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
    )
    supervisor_chain = preprocessor | llm.with_structured_output(nextNode)
    next_node = supervisor_chain.invoke(state).next_node
    
    # if routed back to the same node, exit the loop
    if state.get("next_node") == next_node:
        return FINISH
    return {
        "iteration_count": count,
        "next_node": next_node
    }

#######################################
# Define our multiagent graph structure
#######################################


def agent_node(state, agent, name):
    result = agent.invoke(state)
    return {
        "messages": [
            {
                "role": "assistant",
                "content": result["messages"][-1].content,
                "name": name,
            }
        ]
    }


def final_answer(state):
    prompt = "Usando apenas o conteúdo das mensagens anteriores como contexto, responda à pergunta do usuário. Não adicione nenhum comentário sobre já ter enviado a resposta anteriormente."
    preprocessor = RunnableLambda(
        lambda state: state["messages"] + [{"role": "user", "content": prompt}]
    )
    final_answer_chain = preprocessor | llm
    return {"messages": [final_answer_chain.invoke(state)]}


class AgentState(ChatAgentState):
    next_node: str
    iteration_count: int


def create_agent():
    # tools = [create_vs_tool(**faq_index)]
    # faq_index_node = ChatAgentToolNode(tools)

    # nodes = []
    faq_index_node = functools.partial(rag_node, agent=create_rag_agent(**faq_index), name=faq_index["name"])
    sales_genie_node = functools.partial(agent_node, agent=create_genie_agent(**sales_genie), name=sales_genie["name"])
    log_genie_node = functools.partial(agent_node, agent=create_genie_agent(**log_genie), name=log_genie["name"])
    # node.append(functools.partial(rag_node, agent=create_rag_agent(**faq_index), name=faq_index["name"]))
    # node.append(functools.partial(agent_node, agent=create_genie_agent(**sales_genie), name=sales_genie["name"]))
    # node.append(functools.partial(agent_node, agent=create_genie_agent(**cust_genie), name=cust_genie["name"]))

    workflow = StateGraph(AgentState)
    workflow.add_node("supervisor", supervisor_agent)
    workflow.add_node(faq_index["name"], faq_index_node)
    workflow.add_node(sales_genie["name"], sales_genie_node)
    workflow.add_node(log_genie["name"], log_genie_node)
    # for node in nodes:
    #     workflow.add_node(faq_index["name"], faq_index_node)
    workflow.add_node("final_answer", final_answer)

    workflow.set_entry_point("supervisor")
    # We want our workers to ALWAYS "report back" to the supervisor when done
    for worker in worker_descriptions.keys():
        workflow.add_edge(worker, "supervisor")

    # Let the supervisor decide which next node to go
    workflow.add_conditional_edges(
        "supervisor",
        lambda x: x["next_node"],
        {**{k: k for k in worker_descriptions.keys()}, "FINISH": "final_answer"},
    )
    workflow.add_edge("final_answer", END)
    return workflow.compile()

###################################
# Wrap our multi-agent in ChatAgent
###################################


class LangGraphChatAgent(ChatAgent):
    # def __init__(self, agent: CompiledStateGraph):
    #     self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        
        agent = create_agent()

        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }

        messages = []
        for event in agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        
        agent = create_agent()

        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }
        for event in agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg})
                    for msg in node_data.get("messages", [])
                )


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
mlflow.langchain.autolog()
AGENT = LangGraphChatAgent()
mlflow.models.set_model(AGENT)
