"""Process-isolated OpenAI protocol fixture. Decisions use actual message/tool receipts.

No database access. Pauses occur only at observed child tool-result boundaries;
files let the test kill/restart a worker before releasing the same request.
"""
import json
import pathlib
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

root = pathlib.Path(sys.argv[1])
lock = threading.Lock()

def text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""

def decide(request):
    messages = request["messages"]
    # A new user turn can retain prior dialogue. Only the active fixture task
    # selects the scenario; asynchronous child reports remain in its context.
    starts = [i for i, m in enumerate(messages) if m["role"] == "user" and
              (text(m.get("content")).startswith("[p11:") or
               text(m.get("content")).startswith("Delegated task:\n[p11:"))]
    if starts:
        messages = messages[starts[-1]:]
    users = "\n".join(text(m.get("content")) for m in messages if m["role"] == "user")
    results = "\n".join(text(m.get("content")) for m in messages if m["role"] == "tool")
    calls = [c["function"]["name"] for m in messages for c in m.get("tool_calls", [])]
    offered = [t["function"]["name"] for t in request.get("tools", [])]
    def call(name, args):
        if name not in offered:
            return "tool_catalog_activate", {"names": [name]}
        return name, args
    if "[p11:approval]" in users:
        if "get_current_time" not in calls:
            return call("get_current_time", {"timezone":"UTC"})
        return "text", "Approval outcome observed: " + results
    if "[p11:question]" in users:
        if "interaction_request" not in calls:
            return call("interaction_request", {"kind":"single_select", "question":"Choose a result", "choices":["pass","review"], "allow_other":False, "required":True})
        return "text", "Question response used: " + results
    if "[p11:child:" in users:
        case = users.split("[p11:child:")[1].split("]")[0]
        if "data_python_run" not in calls:
            return call("data_python_run", {"code":"print(6 * 7)"})
        if "42" not in results:
            return "text", "ERROR: actual sandbox receipt missing: " + results
        if case in ("restart", "stop"):
            (root / (case + ".reached")).touch()
            while not (root / (case + ".release")).exists():
                time.sleep(0.025)
        return "text", "child_sandbox_result_42"
    if "[p11:parent:" in users:
        case = users.split("[p11:parent:")[1].split("]")[0]
        if "get_current_time" not in calls:
            return call("get_current_time", {"timezone":"UTC"})
        if "delegation_spawn" not in calls:
            return call("delegation_spawn", {"task":"[p11:child:"+case+"] Run Python to calculate 6 * 7 and report the actual output."})
        if "child_sandbox_result_42" in users:
            return "text", "Parent used child's sandbox result: 42"
        return "text", "Waiting for the child report."
    return "text", "Reference API answer."

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with lock:
            with (root / "model-requests.jsonl").open("a") as out:
                out.write(json.dumps({"idempotency_key":self.headers.get("idempotency-key"),"request":request})+"\n")
        name, value = decide(request)
        call_count = sum(len(m.get("tool_calls", [])) for m in request["messages"])
        delta = {"content":value} if name == "text" else {"tool_calls":[{"index":0,"id":"call_p11_"+str(call_count),"type":"function","function":{"name":name,"arguments":json.dumps(value)}}]}
        chunks = [{"id":"chatcmpl_p11","choices":[{"index":0,"delta":delta,"finish_reason":None}]}, {"id":"chatcmpl_p11","choices":[{"index":0,"delta":{},"finish_reason":"stop" if name=="text" else "tool_calls"}],"usage":{"prompt_tokens":64,"completion_tokens":16,"total_tokens":80}}]
        try:
            self.send_response(200)
            self.send_header("Content-Type","text/event-stream")
            self.end_headers()
            for chunk in chunks:
                self.wfile.write(("data: "+json.dumps(chunk)+"\n\n").encode())
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
    def log_message(self, *_):
        pass

server = ThreadingHTTPServer(("127.0.0.1",0), Handler)
(root / "model-port").write_text(str(server.server_port))
server.serve_forever()
