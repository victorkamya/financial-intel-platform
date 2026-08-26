# Databricks notebook source
# MAGIC %pip install langchain langchain-community langchain-text-splitters databricks-langchain chromadb --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import mlflow
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain_community.vectorstores import Chroma
from databricks_langchain import DatabricksEmbeddings

SEC_FILINGS_PATH = "/Volumes/ml/ml_artifacts/rag_data/sec_filings"
# Chroma's SQLite backend cannot write directly to a UC Volume (FUSE mount
# lacks the POSIX file-locking semantics SQLite needs — confirmed via a
# "disk I/O error" building it at a Volume path). Build on local ephemeral
# disk instead; mlflow.pyfunc.log_model's own artifact packaging durably
# persists the built store into the model registry, so no separate Volume
# copy is needed.
VECTOR_STORE_PATH = "/local_disk0/tmp/chroma_db"
EMBEDDING_ENDPOINT = "databricks-bge-large-en"


@dataclass
class RagConfig:
    volume_path: str
    vector_store_path: str
    chunk_size: int = 1000
    chunk_overlap: int = 200
    collection_name: str = "sec_filings"


class SECFilingProcessor:
    """Process SEC EDGAR 10-K filings (plain text, fetched by
    fetch_sec_filings.py) into a queryable vector store."""

    def __init__(self, config: RagConfig):
        self.config = config
        self.embeddings = DatabricksEmbeddings(endpoint=EMBEDDING_ENDPOINT)
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
            separators=["\n\n", "\n", ".", " "],
        )

    def _load_filings(self) -> Iterator:
        for txt_path in Path(self.config.volume_path).glob("*.txt"):
            loader = TextLoader(str(txt_path))
            docs = loader.load()
            ticker = txt_path.name.split("_10K_")[0]
            for doc in docs:
                doc.metadata["source_file"] = txt_path.name
                doc.metadata["ticker"] = ticker
            yield from docs

    def build_vector_store(self) -> Chroma:
        with mlflow.start_run(run_name="rag_index_build"):
            docs = list(self._load_filings())
            chunks = self.splitter.split_documents(docs)

            mlflow.log_params({
                "num_source_docs": len(docs),
                "num_chunks": len(chunks),
                "chunk_size": self.config.chunk_size,
                "chunk_overlap": self.config.chunk_overlap,
                "embedding_endpoint": EMBEDDING_ENDPOINT,
            })

            vector_store = Chroma.from_documents(
                documents=chunks,
                embedding=self.embeddings,
                collection_name=self.config.collection_name,
                persist_directory=self.config.vector_store_path,
            )

            mlflow.log_metric("chunks_indexed", len(chunks))
            mlflow.set_tag("index_type", "chroma_bge_large")

        return vector_store


# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/vic1771@hotmail.com/ml-pipeline/experiments/financial-analyst-rag")

os.makedirs(VECTOR_STORE_PATH, exist_ok=True)
rag_config = RagConfig(volume_path=SEC_FILINGS_PATH, vector_store_path=VECTOR_STORE_PATH)
processor = SECFilingProcessor(rag_config)
vector_store = processor.build_vector_store()
print(f"Vector store built at {VECTOR_STORE_PATH}")

# COMMAND ----------

import mlflow.pyfunc
# AgentExecutor / create_openai_tools_agent were deprecated in LangChain 0.2
# (moved to the legacy langchain-classic package). Current LangChain (v1)
# builds agents on LangGraph via create_agent instead.
from langchain.agents import create_agent
from langchain.tools import tool
from databricks_langchain import ChatDatabricks
from pyspark.sql import SparkSession

CHAT_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

SYSTEM_PROMPT = (
    "You are a senior financial analyst. Use the available tools to "
    "research companies using SEC filings, price data, and the "
    "user's current portfolio positions. Always cite your sources "
    "with ticker and filing reference when using filing data."
)


class FinancialAnalystAgent(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel wrapping a LangChain agent, registered to Unity Catalog."""

    def load_context(self, context):
        self.spark = SparkSession.builder.getOrCreate()
        self.vector_store = Chroma(
            persist_directory=context.artifacts["vector_store_path"],
            embedding_function=DatabricksEmbeddings(endpoint=EMBEDDING_ENDPOINT),
            collection_name="sec_filings",
        )
        self.retriever = self.vector_store.as_retriever(search_kwargs={"k": 6})
        self.agent = self._build_agent()

    def _build_agent(self):
        llm = ChatDatabricks(endpoint=CHAT_ENDPOINT, max_tokens=2048)

        @tool
        def search_sec_filings(query: str) -> str:
            """Search indexed SEC 10-K filings for relevant financial information."""
            docs = self.retriever.get_relevant_documents(query)
            return "\n\n".join([
                f"[{d.metadata.get('ticker', '?')} | {d.metadata.get('source_file', '?')}]\n{d.page_content}"
                for d in docs
            ])

        @tool
        def get_price_history(ticker: str, days: int = 30) -> str:
            """Fetch recent daily OHLCV price data from the gold layer for a ticker."""
            df = self.spark.sql(f"""
                SELECT trade_date, open, high, low, close, volume
                FROM gold.market_data.gold_fact_daily_ohlcv
                WHERE symbol = '{ticker.upper()}'
                ORDER BY trade_date DESC
                LIMIT {days}
            """)
            pdf = df.toPandas()
            if pdf.empty:
                return f"No price history available yet for {ticker.upper()}."
            return pdf.to_string(index=False)

        @tool
        def get_current_positions(account_id: str = "PA3C0H606N6M") -> str:
            """Fetch the current open positions (symbol, quantity, avg cost) for a paper trading account."""
            df = self.spark.sql(f"""
                SELECT symbol, quantity, avg_cost, event_timestamp
                FROM silver.market_data.silver_positions_current
                WHERE account_id = '{account_id}'
                ORDER BY symbol
            """)
            pdf = df.toPandas()
            if pdf.empty:
                return f"No open positions found for account {account_id}."
            return pdf.to_string(index=False)

        tools = [search_sec_filings, get_price_history, get_current_positions]
        return create_agent(model=llm, tools=tools, system_prompt=SYSTEM_PROMPT)

    def predict(self, context, model_input):
        query = model_input["query"].iloc[0] if hasattr(model_input, "iloc") else model_input["query"][0]
        result = self.agent.invoke({"messages": [{"role": "user", "content": query}]})
        return result["messages"][-1].content


# COMMAND ----------

with mlflow.start_run(run_name="rag_agent_v1") as run:
    mlflow.log_params({
        "llm_endpoint": CHAT_ENDPOINT,
        "embedding_endpoint": EMBEDDING_ENDPOINT,
        "vector_store": "chroma",
        "retriever_k": 6,
        "tools": "search_sec_filings,get_price_history,get_current_positions",
    })

    # Unity Catalog's model registry (unlike the legacy workspace registry)
    # requires an explicit signature at registration time.
    import pandas as pd
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    signature = ModelSignature(
        inputs=Schema([ColSpec("string", "query")]),
        outputs=Schema([ColSpec("string")]),
    )
    input_example = pd.DataFrame({"query": ["What are AAPL's key risk factors?"]})

    model_info = mlflow.pyfunc.log_model(
        artifact_path="financial_analyst_agent",
        python_model=FinancialAnalystAgent(),
        artifacts={"vector_store_path": VECTOR_STORE_PATH},
        registered_model_name="ml.models.financial_analyst_agent",
        signature=signature,
        input_example=input_example,
        pip_requirements=[
            "langchain",
            "langchain-community",
            "langchain-text-splitters",
            "databricks-langchain",
            "chromadb",
        ],
    )

from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name="ml.models.financial_analyst_agent",
    alias="champion",
    version=model_info.registered_model_version,
)
print(f"Registered ml.models.financial_analyst_agent version {model_info.registered_model_version}, alias 'champion' set.")
