import os
import json
import time
import requests
import traceback
import certifi
from flask import Flask, request, jsonify, render_template_string

# --- 配置 ---
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# --- 完整的聊天机器人UI模板 (最终稳定版) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Medicare Physician Services</title>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --bg-color: #ffffff; --text-color: #1a1a1a; --border-color: #e0e0e0; --placeholder-color: #6b7280; --bot-bg: #f7f7f7; --accent-color: #6366f1; }
        body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: var(--bg-color); color: var(--text-color); }
        .chat-container { width: 100%; max-width: 800px; height: 90vh; max-height: 800px; display: flex; flex-direction: column; border: 1px solid var(--border-color); border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); overflow: hidden; position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); }
        .chat-header { padding: 1rem; border-bottom: 1px solid var(--border-color); font-weight: 600; font-size: 1.1rem; display: flex; justify-content: space-between; align-items: center; }
        .new-chat-btn { background: none; border: none; cursor: pointer; color: var(--placeholder-color); padding: 4px; border-radius: 6px; display: flex; align-items: center; justify-content: center; }
        .new-chat-btn:hover { background-color: var(--bot-bg); color: var(--text-color); }
        .chat-messages { flex-grow: 1; overflow-y: auto; padding: 1.5rem; display: flex; flex-direction: column; gap: 1rem; }
        .welcome-screen { text-align: center; margin: auto; }
        .welcome-icon { width: 60px; height: 60px; background: linear-gradient(135deg, #a78bfa, #6366f1); border-radius: 12px; display: flex; align-items: center; justify-content: center; margin: 0 auto 1rem; color: white; }
        .welcome-screen h2 { font-size: 1.5rem; margin-bottom: 0.5rem; }
        .sample-questions { margin-top: 2rem; display: flex; flex-direction: column; gap: 0.75rem; align-items: flex-start; margin-left: auto; margin-right: auto; max-width: 90%; }
        .sample-question { padding: 0.6rem 1rem; border: 1px solid var(--border-color); border-radius: 8px; cursor: pointer; transition: background-color 0.2s; font-size: 0.9rem; display: flex; align-items: center; gap: 0.5rem; text-align: left; }
        .sample-question:hover { background-color: #f9fafb; }
        .message { max-width: 85%; padding: 0.75rem 1.25rem; border-radius: 18px; line-height: 1.5; white-space: pre-wrap; font-family: 'Menlo', 'Consolas', monospace; font-size: 0.9rem; }
        .user-message { background-color: var(--accent-color); color: white; align-self: flex-end; border-bottom-right-radius: 4px; }
        .bot-message { background-color: var(--bot-bg); color: var(--text-color); align-self: flex-start; border-bottom-left-radius: 4px; }
        .bot-message-html { padding: 0; background-color: transparent; align-self: flex-start; max-width: 100%; width:100%; }
        .chart-container-wrapper { align-self: flex-start; width: 100%; }
        .chart-container { background-color: var(--bot-bg); padding: 1rem; border-radius: 18px; width: 100%; box-sizing: border-box; }
        .typing-indicator { align-self: flex-start; display: flex; gap: 4px; padding: 0.75rem 1.25rem; }
        .typing-indicator span { width: 8px; height: 8px; background-color: #ccc; border-radius: 50%; animation: bounce 1s infinite; }
        .typing-indicator span:nth-child(2) { animation-delay: 0.2s; }
        .typing-indicator span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes bounce { 0%, 80%, 100% { transform: scale(0); } 40% { transform: scale(1); } }
        .chat-input-area { border-top: 1px solid var(--border-color); padding: 1rem; display: flex; gap: 0.5rem; align-items: flex-start; }
        .chat-input-area textarea { flex-grow: 1; border: 1px solid var(--border-color); border-radius: 8px; padding: 0.75rem; font-size: 1rem; resize: none; font-family: inherit; max-height: 150px; overflow-y: auto; }
        .chat-input-area textarea:focus { outline: none; border-color: var(--accent-color); }
        .chat-input-area button { border: none; background-color: var(--accent-color); color: white; padding: 0.75rem 1rem; border-radius: 8px; cursor: pointer; display: flex; align-items: center; justify-content: center; height: fit-content; }
        .data-table { border-collapse: collapse; width: 100%; font-family: 'Segoe UI', sans-serif; font-size: 0.9rem; border-radius: 8px; overflow: hidden; background-color: var(--bot-bg); }
        .data-table th, .data-table td { padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border-color); }
        .data-table th { background-color: #f9fafb; font-weight: 600; }
        .data-table tr:last-child td { border-bottom: none; }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header"><span>Medicare Physician Services</span><button class="new-chat-btn" onclick="window.location.reload()" title="New Chat"><i data-lucide="plus-circle"></i></button></div>
        <div class="chat-messages" id="chat-messages">
            <div class="welcome-screen" id="welcome-screen">
                <div class="welcome-icon"><i data-lucide="bot"></i></div><h2>Medicare Physician Services</h2>
                <div class="sample-questions">
                    <div class="sample-question" onclick="askSample(this)"><span>Top 10 providers did RPM?</span></div>
                    <div class="sample-question" onclick="askSample(this)"><span>Services by Scott Stringer?</span></div>
                </div>
            </div>
        </div>
        <div class="chat-input-area"><textarea id="userInput" placeholder="Ask your question..." rows="1" onkeydown="handleEnter(event)"></textarea><button id="sendButton" onclick="sendMessage()"><i data-lucide="send"></i></button></div>
    </div>

    <script>
        lucide.createIcons();
        const chatMessages = document.getElementById('chat-messages');
        const userInput = document.getElementById('userInput');
        const welcomeScreen = document.getElementById('welcome-screen');

        let currentConversationId = null;
        let pendingChartData = null;

        userInput.addEventListener('input', function () { this.style.height = 'auto'; this.style.height = (this.scrollHeight) + 'px'; });
        function askSample(element) { const question = element.querySelector('span').innerText; userInput.value = question; userInput.dispatchEvent(new Event('input', { bubbles: true })); sendMessage(); }
        function handleEnter(event) { if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); sendMessage(); } }
        
        function renderChart(chartData, title, chartType) { 
            const wrapper = document.createElement('div'); wrapper.classList.add('chart-container-wrapper'); 
            const chartContainer = document.createElement('div'); chartContainer.classList.add('chart-container'); 
            const canvas = document.createElement('canvas'); chartContainer.appendChild(canvas); wrapper.appendChild(chartContainer); chatMessages.appendChild(wrapper); 
            new Chart(canvas, { type: chartType, data: chartData, options: { responsive: true, plugins: { legend: { position: 'top' }, title: { display: true, text: title } }, scales: { y: { beginAtZero: false, ticks: { callback: function(value) { if (Math.abs(value) >= 1e6) return (value / 1e6).toFixed(2) + 'M'; if (Math.abs(value) >= 1e3) return (value / 1e3).toFixed(2) + 'K'; return value; } } } } } }); 
            chatMessages.scrollTop = chatMessages.scrollHeight; 
        }

        function addMessage(content, sender) { const messageElement = document.createElement('div'); messageElement.classList.add('message', `${sender}-message`); messageElement.innerText = content; chatMessages.appendChild(messageElement); chatMessages.scrollTop = chatMessages.scrollHeight; }
        function addHtmlContent(html, sender) { const messageElement = document.createElement('div'); messageElement.classList.add('message', `${sender}-message-html`); messageElement.innerHTML = html; chatMessages.appendChild(messageElement); chatMessages.scrollTop = chatMessages.scrollHeight; }
        function showTypingIndicator() { const indicator = document.createElement('div'); indicator.id = 'typing-indicator'; indicator.classList.add('typing-indicator'); indicator.innerHTML = '<span></span><span></span><span></span>'; chatMessages.appendChild(indicator); chatMessages.scrollTop = chatMessages.scrollHeight; }
        function removeTypingIndicator() { const indicator = document.getElementById('typing-indicator'); if (indicator) indicator.remove(); }
        
        async function sendMessage() {
            const question = userInput.value.trim();
            const questionLower = question.toLowerCase();
            if (!question) return;
            
            if (pendingChartData) {
                let requestedChartType = null;
                if (questionLower.includes('bar') || questionLower.includes('histogram') || questionLower.includes('柱状图')) {
                    requestedChartType = 'bar';
                } else if (questionLower.includes('line') || questionLower.includes('折线图')) {
                    requestedChartType = 'line';
                }

                if (requestedChartType) {
                    addMessage(question, 'user');
                    userInput.value = ''; userInput.style.height = 'auto';
                    addMessage("Of course. Here is the chart you requested:", 'bot');
                    renderChart(pendingChartData.data, pendingChartData.title, requestedChartType);
                    pendingChartData = null;
                    return;
                }
            }

            pendingChartData = null;
            if (welcomeScreen) welcomeScreen.style.display = 'none';

            addMessage(question, 'user');
            userInput.value = ''; userInput.style.height = 'auto';
            showTypingIndicator();

            try {
                const res = await fetch('/ask', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question: question, conversation_id: currentConversationId })
                });
                
                // This line is CRITICAL. We check if the response is ok before trying to parse it as JSON.
                if (!res.ok) {
                    // If we get a 500 error, the response text itself is the server's HTML error page.
                    // We show a user-friendly message instead of trying to parse it.
                    throw new Error(`Server responded with status: ${res.status}`);
                }

                const data = await res.json(); // Now this line is safe.
                
                removeTypingIndicator();
                if (data.conversation_id) currentConversationId = data.conversation_id;

                if (data.error) {
                    addMessage(`Error: ${data.details || data.error}`, 'bot');
                } 
                else if (data.type === 'text_and_table_with_chart_data') {
                    if (data.content) addMessage(data.content, 'bot');
                    if (data.table_html) addHtmlContent(data.table_html, 'bot');
                    pendingChartData = { data: data.chart_data, title: data.title };
                }
                else if (data.type === 'text' && data.content) {
                    addMessage(data.content, 'bot');
                }

            } catch (error) {
                removeTypingIndicator();
                // Display a clean, generic error message for any kind of failure.
                addMessage('An unexpected error occurred. Please check the server logs for details. Error: ' + error.message, 'bot');
            }
        }
    </script>
</body>
</html>
"""

# --- Python 后端部分 ---
app = Flask(__name__)

def create_html_table(columns, data_array):
    html = "<table class='data-table'><thead><tr>"
    for col in columns:
        html += f"<th>{col['name'].replace('_', ' ').title()}</th>"
    html += "</tr></thead><tbody>"
    for row in data_array:
        html += "<tr>"
        for i, cell in enumerate(row):
            cell_val = cell if cell is not None else ""
            html += f"<td>{cell_val}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    return html

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/ask', methods=['POST'])
def ask():
    # --- MODIFICATION START: The entire function is now wrapped in a try...except block ---
    try:
        if not all([DATABRICKS_HOST, GENIE_SPACE_ID, DATABRICKS_TOKEN]):
            return jsonify({"error": "Server is not configured. Missing environment variables."}), 500
        
        # This can fail if the request is not JSON, so it must be inside the try block.
        request_data = request.get_json()
        if not request_data:
            return jsonify({'error': 'Invalid request: Missing or malformed JSON body.'}), 400

        user_question = request_data.get('question')
        conversation_id = request_data.get('conversation_id')

        if not user_question:
            return jsonify({'error': 'Question cannot be empty'}), 400

        headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
        ssl_verify_path = certifi.where()
        
        message_id = None
        if not conversation_id:
            start_conv_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
            start_payload = {'content': user_question}
            start_response = requests.post(start_conv_url, headers=headers, json=start_payload, verify=ssl_verify_path)
            start_response.raise_for_status()
            start_data = start_response.json()
            conversation_id = start_data['conversation']['id']
            message_id = start_data['message']['id']
        else:
            add_message_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages"
            add_payload = {'content': user_question}
            add_response = requests.post(add_message_url, headers=headers, json=add_payload, verify=ssl_verify_path)
            add_response.raise_for_status()
            add_data = add_response.json()
            message_id = add_data['id']

        message_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        status = ""
        poll_data = {}
        start_time = time.time()
        while status not in ['COMPLETED', 'FAILED', 'CANCELLED'] and time.time() - start_time < 300:
            time.sleep(3)
            poll_response = requests.get(message_url, headers=headers, verify=ssl_verify_path)
            poll_response.raise_for_status()
            poll_data = poll_response.json()
            status = poll_data.get('status')
        
        base_response = {"conversation_id": conversation_id}

        if status == 'COMPLETED':
            text_parts = []
            final_response_generated = False
            
            for attachment in poll_data.get('attachments', []):
                if 'text' in attachment:
                    content = attachment['text']
                    if isinstance(content, str):
                        text_parts.append(content)
                    elif isinstance(content, dict) and 'content' in content:
                        text_parts.append(content['content'])

                if 'query' in attachment and 'statement_id' in attachment['query']:
                    statement_id = attachment['query']['statement_id']
                    results_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
                    results_response = requests.get(results_url, headers=headers, verify=ssl_verify_path)
                    
                    if results_response.status_code == 200:
                        results_data = results_response.json()
                        manifest = results_data.get('manifest', {})
                        result = results_data.get('result', {})
                        columns = manifest.get('schema', {}).get('columns', [])
                        data_array = result.get('data_array', [])

                        if len(columns) >= 2 and data_array:
                            data_col = columns[-1]
                            data_col_type = data_col['type_name'].lower()
                            is_numeric = any(t in data_col_type for t in ['long', 'int', 'double', 'float', 'decimal'])
                            
                            if is_numeric:
                                html_table = create_html_table(columns, data_array)
                                labels = [" ".join(map(str, row[:-1])) for row in data_array]
                                data_points = [float(row[-1]) for row in data_array]
                                chart_data = {'labels': labels, 'datasets': [{'label': data_col['name'].replace('_', ' ').title(), 'data': data_points, 'borderColor': '#6366f1', 'backgroundColor': '#6366f1'}]}
                                
                                base_response.update({
                                    'type': 'text_and_table_with_chart_data',
                                    'title': f"Chart of {chart_data['datasets'][0]['label']}",
                                    'content': "\n\n".join(text_parts),
                                    'table_html': html_table,
                                    'chart_data': chart_data,
                                })
                                final_response_generated = True
                                break 
            
            if final_response_generated:
                return jsonify(base_response)

            if text_parts:
                base_response.update({'type': 'text', 'content': "\n\n".join(text_parts)})
                return jsonify(base_response)
            else:
                base_response.update({'type': 'text', 'content': "I've processed your request, but couldn't find a specific answer or data."})
                return jsonify(base_response)
        else:
            return jsonify({'error': f'Failed to get answer. Final status: {status}', 'details': poll_data.get('error')}), 500

    except Exception as e:
        # This is the crucial part. Any error, including getting the request JSON, will be caught here.
        error_details = traceback.format_exc()
        # It's good practice to log the full error on the server for debugging.
        print(f"--- UNHANDLED EXCEPTION --- \n{error_details}")
        return jsonify({'error': 'An unexpected server error occurred.', 'details': str(e)}), 500
    # --- MODIFICATION END ---

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
