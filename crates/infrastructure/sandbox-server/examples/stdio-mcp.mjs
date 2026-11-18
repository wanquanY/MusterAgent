// Dependency-free Node MCP example. Network transport is explicitly proxy-aware.
import readline from 'node:readline';
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';

async function get(url) {
  const target = new URL(url);
  if (target.protocol !== 'https:') throw new Error('HTTPS required');
  const proxy = new URL(process.env.HTTPS_PROXY ?? '');
  return new Promise((resolve, reject) => {
    const connect = http.request({hostname: proxy.hostname, port: proxy.port,
      method: 'CONNECT', path: target.host + (target.port ? '' : ':443')});
    connect.setTimeout(10000, () => connect.destroy(new Error('proxy timeout')));
    connect.on('error', reject);
    connect.on('connect', (response, socket) => {
      if (response.statusCode !== 200) {
        socket.destroy(); reject(new Error('proxy denied: ' + response.statusCode)); return;
      }
      const agent = new https.Agent({keepAlive: false});
      let secure;
      agent.createConnection = () => {
        secure = tls.connect({socket, servername: target.hostname});
        secure.on('error', reject);
        return secure;
      };
      const request = https.request(target, {agent}, res => {
        let bytes = 0;
        res.on('data', chunk => { bytes += chunk.length; if (bytes > 1048576) res.destroy(new Error("response byte limit")); });
        res.on('end', () => { secure.destroy(); resolve({status: res.statusCode, bytes}); });
        res.on('error', reject);
      });
      request.on('error', reject);
      request.setTimeout(10000, () => request.destroy(new Error('request timeout')));
      request.end();
    });
    connect.end();
  });
}
const tools = [
  {name:'add', description:'Add two numbers', inputSchema:{type:'object',properties:{a:{type:'number'},b:{type:'number'}},required:['a','b']}},
  {name:'fetch', description:'HTTPS GET through the sandbox proxy',inputSchema:{type:'object',properties:{url:{type:'string'}},required:['url']}}
];
for await (const line of readline.createInterface({input: process.stdin})) {
  const request = JSON.parse(line);
  if (request.id === undefined) continue;
  try {
    let result;
    switch (request.method) {
      case 'initialize': result={protocolVersion:'2025-03-26',capabilities:{tools:{}},serverInfo:{name:'sandbox-example',version:'1'}}; break;
      case 'tools/list': result={tools}; break;
      case 'tools/call': {
        const args=request.params.arguments;
        const value=request.params.name==='add' ? args.a+args.b : await get(args.url);
        result={content:[{type:'text',text:JSON.stringify(value)}]}; break;
      }
      case 'ping': result={}; break;
      default: throw new Error('unknown method');
    }
    process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:request.id,result})+'\n');
  } catch (error) {
    process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:request.id,error:{code:-32000,message:error.message}})+'\n');
  }
}
