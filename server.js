// Callyn Server — WebSocket relay de áudio PCM
// Recebe áudio binário de um cliente e retransmite pro outro
// Também gerencia mensagens de controle (call, answer, hangup)

const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Mapa: uid -> { ws, name, inCallWith }
const clients = new Map();

console.log(`[Callyn] Servidor rodando na porta ${PORT}`);

wss.on('connection', (ws) => {
    let myUid = null;

    console.log(`[Callyn] Nova conexão`);

    ws.on('message', (data, isBinary) => {
        // Se for binário, é áudio. Retransmite pro parceiro da call.
        if (isBinary) {
            if (!myUid) return;
            const me = clients.get(myUid);
            if (!me || !me.inCallWith) return;
            const partner = clients.get(me.inCallWith);
            if (!partner || partner.ws.readyState !== WebSocket.OPEN) return;
            partner.ws.send(data, { binary: true });
            return;
        }

        // Se for texto, é JSON de controle
        let msg;
        try {
            msg = JSON.parse(data.toString());
        } catch (e) {
            console.log(`[Callyn] JSON inválido: ${e.message}`);
            return;
        }

        console.log(`[Callyn] Mensagem: ${JSON.stringify(msg)}`);

        switch (msg.type) {
            case 'register':
                // Cliente se registra com uid e nome
                myUid = msg.uid;
                clients.set(myUid, {
                    ws: ws,
                    name: msg.name || 'Anônimo',
                    inCallWith: null,
                });
                console.log(`[Callyn] Registrado: ${myUid} (${msg.name})`);

                // Manda lista de online pro cliente
                const onlineList = Array.from(clients.entries())
                    .filter(([uid]) => uid !== myUid)
                    .map(([uid, c]) => ({ uid, name: c.name }));
                ws.send(JSON.stringify({ type: 'online', users: onlineList }));
                break;

            case 'call':
                // Um cliente liga pro outro
                const targetUid = msg.target;
                const target = clients.get(targetUid);
                if (!target) {
                    ws.send(JSON.stringify({ type: 'error', message: 'Usuário offline' }));
                    return;
                }
                const caller = clients.get(myUid);
                if (!caller) {
                    ws.send(JSON.stringify({ type: 'error', message: 'Você não está registrado' }));
                    return;
                }
                caller.inCallWith = targetUid;
                // Avisa o alvo que tem chamada
                target.ws.send(JSON.stringify({
                    type: 'incoming',
                    fromUid: myUid,
                    fromName: caller.name,
                }));
                console.log(`[Callyn] ${myUid} ligando pra ${targetUid}`);
                break;

            case 'answer':
                // O alvo atendeu
                const callerUid = msg.target;
                const callerClient = clients.get(callerUid);
                if (!callerClient) return;
                const meClient = clients.get(myUid);
                if (!meClient) return;
                meClient.inCallWith = callerUid;
                callerClient.ws.send(JSON.stringify({
                    type: 'answered',
                    fromUid: myUid,
                }));
                console.log(`[Callyn] ${myUid} atendeu ${callerUid}`);
                break;

            case 'hangup':
                // Encerra a call
                const meH = clients.get(myUid);
                if (meH && meH.inCallWith) {
                    const partnerUid = meH.inCallWith;
                    const partnerH = clients.get(partnerUid);
                    if (partnerH) {
                        partnerH.ws.send(JSON.stringify({ type: 'ended', fromUid: myUid }));
                        partnerH.inCallWith = null;
                    }
                    meH.inCallWith = null;
                }
                console.log(`[Callyn] ${myUid} encerrou`);
                break;

            default:
                console.log(`[Callyn] Tipo desconhecido: ${msg.type}`);
        }
    });

    ws.on('close', () => {
        if (myUid) {
            const me = clients.get(myUid);
            if (me && me.inCallWith) {
                const partner = clients.get(me.inCallWith);
                if (partner) {
                    partner.ws.send(JSON.stringify({ type: 'ended', fromUid: myUid }));
                    partner.inCallWith = null;
                }
            }
            clients.delete(myUid);
            console.log(`[Callyn] Desconectado: ${myUid}`);
        }
    });

    ws.on('error', (err) => {
        console.log(`[Callyn] Erro: ${err.message}`);
    });
});
