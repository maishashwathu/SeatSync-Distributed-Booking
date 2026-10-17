const $ = id => document.getElementById(id);
let state, selected = null, busy = false, requestKey = null, pollRunning = false;
const esc = s => String(s ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(message) { $('toast').textContent = message; $('toast').hidden = false; clearTimeout(toast.timer); toast.timer = setTimeout(()=>$('toast').hidden=true,5000); }
async function api(path, data) {
  const response = await fetch('/api/'+path, data === undefined ? {} : {method:'POST', headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  return response.json();
}
function render() {
  $('booked-count').innerHTML = `${Object.keys(state.bookings).length}<span> / 48</span>`;
  $('leader-metric').textContent = state.leader ? `Node 0${state.leader}` : 'Electing…';
  $('latency').innerHTML = `${state.metrics.latency_ms}<span> ms</span>`;
  $('messages').textContent = state.metrics.messages.toLocaleString();
  const online = state.nodes.filter(n=>n.online);
  $('cluster-badge').textContent = !state.quorum ? 'No quorum · bookings paused' : !state.leader ? 'Election in progress' : online.length === 3 ? '● All systems operational' : '● Degraded · quorum available';
  $('cluster-badge').classList.toggle('degraded',online.length<3 || !state.leader);
  $('online-count').textContent = `${online.length} / 3 online`;
  const same = online.length>1 && online.every(n=>JSON.stringify(n.bookings)===JSON.stringify(online[0].bookings));
  $('sync-label').textContent = same ? `${online.length} replicas hold identical booking records` : online.length<2 ? 'Waiting for a second replica' : 'Replicas are synchronizing';
  $('sync-dot').style.background = same ? '#389b6b' : '#bc954e';
  $('nodes').innerHTML = state.nodes.map(n=>`<div class="node ${n.role}"><div class="node-icon">▤</div><div><div class="node-name">Node 0${n.id}${n.role==='leader'?'<b>COORDINATOR</b>':''}</div><div class="node-sub">127.0.0.1:${n.port} · ${esc(n.role)}</div></div><div class="node-end"><div class="node-version">${n.online?`${Object.keys(n.bookings).length} bookings`:'Offline'}<small>${n.online?`term ${n.term} · seq ${n.seq}`:'Process stopped'}</small></div><button class="node-control" data-node="${n.id}" data-action="${n.online?'stop':'start'}" ${busy?'disabled':''}>${n.online?'Stop':'Restart'}</button></div></div>`).join('');
  if (selected && state.bookings[selected]) selected = null;
  $('seatmap').innerHTML = [...'ABCDEF'].map(row=>`<span class="row-label">${row}</span>`+Array.from({length:8},(_,i)=>{
    const seat=row+(i+1), booking=state.bookings[seat];
    return `<button class="seat ${booking?'booked':selected===seat?'selected':''}" data-seat="${seat}" aria-label="Seat ${seat}${booking?', reserved':', available'}" aria-pressed="${selected===seat}" ${booking?'disabled':''} title="${booking?`Reserved for ${esc(booking.name)}`:`Select ${seat}`}">${i+1}</button>`;
  }).join('')).join('');
  $('selection-label').textContent = selected ? `Seat ${selected} · ₹499` : 'Select a seat above';
  $('reserve').disabled = busy || !selected || !state.quorum || !state.leader;
  $('race').disabled = busy || !selected || !state.quorum || !state.leader;
  $('failover').disabled = busy || online.length<3 || !state.leader;
  $('entry-node').querySelectorAll('option').forEach(o=>o.disabled=!state.nodes[Number(o.value)-1].online);
  if (!state.nodes[Number($('entry-node').value)-1].online && online.length) $('entry-node').value=online[0].id;
  $('events').innerHTML = state.events.slice(0,35).map(e=>`<tr><td>${new Date(e.time*1000).toLocaleTimeString('en-GB')}</td><td>Node 0${e.node}</td><td>L = ${e.clock}</td><td><span class="event-type ${esc(e.kind)}">${esc(e.kind)}</span></td><td>${esc(e.detail)}</td></tr>`).join('') || '<tr><td colspan="5">Waiting for cluster events…</td></tr>';
  if (state.last_race || state.last_failover) {
    const race=state.last_race, fail=state.last_failover;
    $('experiment-result').innerHTML = (race ? `<b>Race for ${esc(race.seat)}:</b> ${race.successes} confirmed, ${race.conflicts} conflicts, ${race.errors} errors. Average response ${race.average_ms} ms.<br>`:'') + (fail ? `<b>Failover:</b> Node 0${fail.old_leader} → Node 0${fail.new_leader} in ${fail.duration_ms} ms. ${fail.bookings_preserved} booking(s) preserved.`:'');
  } else $('experiment-result').textContent='Select an available seat, then run the race to see mutual exclusion in action.';
}
async function refresh(){if(pollRunning)return;pollRunning=true;try{state=await api('state');render();}catch{ $('cluster-badge').textContent='Dashboard disconnected';$('cluster-badge').classList.add('degraded');$('reserve').disabled=$('race').disabled=$('failover').disabled=true;}finally{pollRunning=false;}}
async function action(path,data){if(busy)return;busy=true;if(state)render();try{const result=await api(path,data);if(!result.ok)throw Error(result.error);return result;}catch(error){toast(error.message);}finally{busy=false;await refresh();}}
$('seatmap').addEventListener('click',e=>{const seat=e.target.closest('[data-seat]');if(seat&&!seat.disabled){selected=seat.dataset.seat;requestKey=null;render();}});
$('guest').addEventListener('input',()=>requestKey=null);
$('booking-form').addEventListener('submit',async e=>{e.preventDefault();if(!selected)return;requestKey ||= crypto.randomUUID();const result=await action('book',{seat:selected,name:$('guest').value.trim(),node:Number($('entry-node').value),request_id:requestKey});if(result?.ok){$('receipt').className='receipt';$('receipt').textContent=`Reservation confirmed · ${result.booking.id} · Seat ${result.booking.seat} · ${result.booking.name}. Coordinated by Node 0${result.booking.coordinator}.`;$('receipt').hidden=false;requestKey=null;selected=null;render();}});
$('race').onclick=async()=>{if(!selected)return;const result=await action('race',{seat:selected});if(result?.ok)toast(`${result.race.requests} contenders: ${result.race.successes} reservation, ${result.race.conflicts} conflicts.`);};
$('failover').onclick=async()=>{toast('Stopping the coordinator. Waiting for election and recovery…');const result=await action('failover',{});if(result?.ok)toast(`Node 0${result.failover.new_leader} took over in ${result.failover.duration_ms} ms.`);};
$('nodes').onclick=async e=>{const b=e.target.closest('[data-node]');if(b){await action(b.dataset.action,{node:Number(b.dataset.node)});}};
$('export').onclick=async()=>{try{const data=await api('export');const link=document.createElement('a');const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));link.href=url;link.download='seatsync-evidence.json';link.click();URL.revokeObjectURL(url);}catch{toast('Could not export evidence. Check the local server.');}};
$('reset').onclick=()=>$('reset-dialog').showModal();
$('cancel-reset').onclick=()=>$('reset-dialog').close();
$('confirm-reset').onclick=async()=>{$('reset-dialog').close();await action('reset',{});$('receipt').hidden=true;selected=null;requestKey=null;await refresh();toast('Demo reset. All seats are available.');};
refresh();setInterval(refresh,1400);
