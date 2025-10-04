import mqtt from 'mqtt';
const url = 'mqtt://broker.emqx.io:1883'; // หรือ ws://broker.emqx.io:8083/mqtt
const client = mqtt.connect(url, { clientId:'test-'+Date.now(), protocolVersion:4, clean:true, keepalive:60, reconnectPeriod:0 });
client.on('connect', (ack)=>{ console.log('CONNECTED', ack); client.end(); });
client.on('error', (e)=>{ console.error('ERROR', e); });
client.on('close', ()=>{ console.log('CLOSE'); });
