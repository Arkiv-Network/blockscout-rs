const ws = new WebSocket("ws://localhost:8050/ws/");
let connected = false;
ws.onopen = () => {
    connected = true;
    console.log("Connected to server");
};
ws.onmessage = (e) => console.log("Message:", e.data);
ws.onclose = () => {
    connected = false;
    console.log("Disconnected from server");
};
setInterval(() => {
    if (connected) {
        ws.send("hello");
    }
}, 1000);