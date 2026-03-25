import faulthandler
import time
import traceback

from app.services.report_agent import ReportAgent


def main():
    faulthandler.enable()
    faulthandler.dump_traceback_later(180, repeat=False)

    simulation_requirement = "Will the price of Bitcoin be above $70,000 on March 21st gmt+3 at 7:00pm"
    query = (
        "Bitcoin price threshold validation failure due to timestamp data mismatch "
        "across information networks on March 21st GMT+3 7:00pm"
    )
    report_context = (
        "Temporal Data Mismatch and Validation Failure section - need to understand "
        "what temporal misalignment occurred, how validation failed, and agent reactions"
    )

    agent = ReportAgent(
        graph_id="mirofish_2bffac98f8e047c7",
        simulation_id="sim_d34681d228a9",
        simulation_requirement=simulation_requirement,
    )

    svc = agent.zep_tools

    orig_query_rag = svc._query_rag

    def wrapped_query_rag(graph_id, query_text, mode="hybrid"):
        print(f"QUERY_RAG start mode={mode} query={query_text[:120]!r}", flush=True)
        started = time.time()
        result = orig_query_rag(graph_id, query_text, mode)
        print(
            f"QUERY_RAG done mode={mode} secs={time.time() - started:.2f} "
            f"len={len(result) if result else 0}",
            flush=True,
        )
        return result

    svc._query_rag = wrapped_query_rag

    llm_client = svc.llm
    orig_chat = llm_client.chat

    def wrapped_chat(messages, **kwargs):
        tail = messages[-1]["content"] if messages else ""
        print(f"LLM chat start tail={tail[:160]!r}", flush=True)
        started = time.time()
        result = orig_chat(messages, **kwargs)
        print(
            f"LLM chat done secs={time.time() - started:.2f} "
            f"len={len(result) if result else 0}",
            flush=True,
        )
        return result

    llm_client.chat = wrapped_chat

    started = time.time()
    try:
        result = agent._execute_tool(
            "insight_forge",
            {"query": query, "report_context": report_context},
            report_context=report_context,
        )
        print(f"FINAL RESULT LEN={len(result)}", flush=True)
        print(result[:4000], flush=True)
    except Exception as exc:
        print(f"EXCEPTION: {exc}", flush=True)
        traceback.print_exc()
    finally:
        print(f"TOTAL SECONDS={time.time() - started:.2f}", flush=True)


if __name__ == "__main__":
    main()
