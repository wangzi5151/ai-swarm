"""手机/桌面浏览器直接打开的看板（单文件，无外部依赖）。"""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b0f14">
<title>Swarm 协作看板</title>
<style>
  :root{
    --bg:#0b0f14; --panel:#131a23; --panel2:#182230; --line:#22303f;
    --txt:#e6edf3; --dim:#8b9bb0; --accent:#4ea1ff;
    --ok:#3fb950; --warn:#d29922; --bad:#f85149; --run:#58a6ff; --idle:#6e7681;
  }
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
  body{margin:0;background:var(--bg);color:var(--txt);
    font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Noto Sans CJK SC",sans-serif;
    padding:12px 12px 60px}
  h1{font-size:17px;margin:0 0 2px;letter-spacing:.3px}
  h2{font-size:13px;margin:0 0 8px;color:var(--dim);font-weight:600;letter-spacing:.5px;text-transform:uppercase}
  .top{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:12px}
  .sub{color:var(--dim);font-size:12px}
  .pill{display:inline-block;padding:1px 7px;border-radius:99px;font-size:11px;border:1px solid var(--line);color:var(--dim)}
  .grid{display:grid;gap:10px}
  @media(min-width:820px){.grid.two{grid-template-columns:1.1fr .9fr}}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px}
  .stats{display:flex;gap:14px;flex-wrap:wrap}
  .stat b{font-size:20px;font-variant-numeric:tabular-nums}
  .stat span{display:block;font-size:11px;color:var(--dim)}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  .node{display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #1b2531}
  .node:last-child{border-bottom:none}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--idle);flex:none}
  .dot.on{background:var(--ok);box-shadow:0 0 8px rgba(63,185,80,.7)}
  .dot.busy{background:var(--run);box-shadow:0 0 8px rgba(88,166,255,.7)}
  .task{border:1px solid var(--line);border-radius:9px;padding:8px 10px;margin-bottom:6px;background:var(--panel2)}
  .task .row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
  .task .name{font-weight:600;flex:1;min-width:120px}
  .tag{font-size:11px;padding:1px 6px;border-radius:5px;border:1px solid var(--line);color:var(--dim)}
  .st-done{color:var(--ok);border-color:#1f6b34}
  .st-running,.st-preparing{color:var(--run);border-color:#1f4f7a}
  .st-queued{color:var(--dim)}
  .st-failed{color:var(--bad);border-color:#7a2222}
  .st-conflict{color:var(--warn);border-color:#7a5a1f}
  .st-blocked{color:var(--warn);border-color:#7a5a1f;border-style:dashed}
  .st-cancelled{color:var(--idle);border-style:dashed}
  .bar{height:5px;border-radius:99px;background:#1b2531;overflow:hidden;margin-top:7px}
  .bar>i{display:block;height:100%;background:linear-gradient(90deg,#4ea1ff,#3fb950);width:0;transition:width .4s}
  .log{color:var(--dim);font-size:11.5px;margin-top:5px;max-height:120px;overflow:auto;white-space:pre-wrap;word-break:break-all}
  .conf{padding:6px 8px;border:1px solid #7a5a1f;border-radius:8px;margin-top:6px;font-size:12px;background:#1e1a10}
  .conf button{margin-right:6px;margin-top:6px}
  button{background:#1f2b3a;color:var(--txt);border:1px solid var(--line);border-radius:8px;
    padding:6px 12px;font-size:12.5px;cursor:pointer}
  button:active{background:#28374a}
  button.primary{background:#1c4f8a;border-color:#2a6db8}
  button.danger{background:#5c1f1f;border-color:#8a2b2b}
  button:disabled{opacity:.45}
  input,textarea{width:100%;background:#0e141b;color:var(--txt);border:1px solid var(--line);
    border-radius:8px;padding:8px;font-size:13px;font-family:inherit}
  textarea{min-height:70px;resize:vertical}
  .muted{color:var(--dim)}
  .evt{font-size:11.5px;color:var(--dim);border-bottom:1px solid #1b2531;padding:3px 0}
  .evt b{color:#a9b7c8;font-weight:500}
  .warnrow{color:var(--warn)}
  .errrow{color:var(--bad)}
  .right{float:right}
  .foot{position:fixed;left:0;right:0;bottom:0;padding:6px 12px;background:rgba(11,15,20,.92);
    border-top:1px solid var(--line);font-size:11px;color:var(--dim);display:flex;gap:10px;justify-content:space-between}
  #diffbox{display:none;margin-top:10px}
  #diffpre{max-height:50vh;overflow:auto;background:#0e141b;border:1px solid var(--line);
    border-radius:8px;padding:8px;font-size:12px;white-space:pre-wrap;word-break:break-all}
</style>
</head>
<body>
<div class="top">
  <h1 id="hubname">Swarm</h1>
  <span class="sub" id="hubmeta"></span>
  <span class="right"><a href="#" id="tokreset" class="sub">换令牌</a></span>
</div>

<div class="grid two" style="margin-bottom:10px">
  <div class="card">
    <h2>节点（同局域网）</h2>
    <div id="nodes"><div class="muted">等待节点接入…</div></div>
  </div>
  <div class="card">
    <h2>总览</h2>
    <div class="stats" id="stats"></div>
    <div style="margin-top:10px" class="sub" id="tick"></div>
  </div>
</div>

<div class="card" style="margin-bottom:10px">
  <h2>项目 <span class="pill" id="projcount">0</span></h2>
  <div id="projects"><div class="muted">还没有项目。在 hub 机器上执行 <code>./swarm submit</code>，或展开下面的表单。</div></div>
  <div id="diffbox">
    <h2>差异 <span class="pill" id="difftitle"></span> <a href="#" class="sub" id="diffclose">关闭</a></h2>
    <pre id="diffpre"></pre>
  </div>
  <details style="margin-top:10px">
    <summary class="sub" style="cursor:pointer">＋ 新建项目（拆解并派发）</summary>
    <div style="margin-top:8px;display:grid;gap:8px">
      <input id="ntitle" placeholder="项目名称，比如：局域网记账 App">
      <textarea id="nspec" placeholder="把需求写在这里（markdown）。可用 '## 模块名 (src/xxx/**)' 指定每个模块的目录。"></textarea>
      <div><button class="primary" id="nsubmit">拆解并开始</button></div>
    </div>
  </details>
</div>

<div class="card" style="margin-bottom:10px">
  <h2>节点令牌 <span class="pill" id="tokcount">0</span></h2>
  <div id="tokens" class="muted">加载中…</div>
  <div style="display:flex;gap:6px;margin-top:8px">
    <input id="tokname" placeholder="令牌名字，比如 手机A">
    <button class="primary" id="tokadd">发放</button>
  </div>
  <div id="toknew" class="sub" style="margin-top:8px;word-break:break-all"></div>
</div>

<div class="card">
  <h2>事件</h2>
  <div id="events" class="log" style="max-height:220px"></div>
</div>

<div class="foot"><span id="conn">连接中…</span><span>Swarm 局域网协作</span></div>

<script>
(function(){
  var state = null;
  var askedToken = false;

  function $(id){ return document.getElementById(id); }
  function esc(s){ return String(s==null?'':s).replace(/[&<>"']/g,function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function api(path, opts){
    opts = opts || {};
    opts.credentials = 'same-origin';
    opts.headers = Object.assign({'Content-Type':'application/json'}, opts.headers||{});
    return fetch(path, opts).then(function(r){
      if(r.status===401){ needToken(); throw new Error('unauthorized'); }
      if(r.status===403){ throw new Error('forbidden'); }
      return r.json().then(function(obj){
        if(!r.ok){ throw new Error((obj&&obj.error)||('HTTP '+r.status)); }
        return obj;
      });
    });
  }
  function needToken(){
    if(askedToken) return;
    askedToken = true;
    var t = prompt('输入 hub 启动时打印的主令牌（Token）：', '');
    if(t){ location.href = '/?t=' + encodeURIComponent(t.trim()); }
  }

  var stColor = {done:'st-done', running:'st-running', preparing:'st-preparing',
                 queued:'st-queued', failed:'st-failed', conflict:'st-conflict',
                 blocked:'st-blocked', cancelled:'st-cancelled'};
  var stName = {done:'已完成', running:'进行中', preparing:'准备中', queued:'排队',
                failed:'失败', conflict:'待裁决', blocked:'被阻塞', cancelled:'已取消'};
  var pjName = {running:'进行中', done:'已完成', failed:'失败/阻塞', review:'待裁决',
                paused:'已暂停', cancelled:'已取消'};

  function render(s){
    state = s;
    $('hubname').textContent = s.hub.name;
    $('hubmeta').textContent = 'v' + s.hub.version + ' · 运行 ' + Math.round(s.hub.uptime) + 's';
    if(!s.nodes.length){ $('nodes').innerHTML = '<div class="muted">等待节点接入…</div>'; }
    else{
      $('nodes').innerHTML = s.nodes.map(function(n){
        var busy = n.running.length>0;
        var cls = n.online ? (busy?'busy':'on') : '';
        return '<div class="node"><span class="dot '+cls+'"></span>'+
          '<div style="flex:1"><b>'+esc(n.name)+'</b> <span class="sub">'+esc(n.addr)+'</span>'+
          '<div class="sub">'+(n.online?('空闲 '+n.slots+' 槽 · 在跑 '+n.running.length):('失联 '+n.last_seen+'s'))+
          ' · 完成 '+n.done+' · 失败 '+n.failed+' · '+esc(n.kinds.join(','))+'</div></div></div>';
      }).join('');
    }
    var run=0, q=0, done=0, fail=0, conf=0, blocked=0;
    s.projects.forEach(function(p){
      run += p.counts.running||0; q += p.counts.queued||0;
      done += p.counts.done||0; fail += p.counts.failed||0;
      conf += p.counts.conflict||0; blocked += p.counts.blocked||0;
    });
    $('stats').innerHTML =
      '<div class="stat"><b>'+s.nodes.filter(function(n){return n.online}).length+'</b><span>在线节点</span></div>'+
      '<div class="stat"><b>'+run+'</b><span>进行中</span></div>'+
      '<div class="stat"><b>'+q+'</b><span>排队</span></div>'+
      '<div class="stat"><b>'+done+'</b><span>已完成</span></div>'+
      '<div class="stat"><b>'+conf+'</b><span>待裁决</span></div>'+
      '<div class="stat"><b>'+fail+'</b><span>失败</span></div>'+
      '<div class="stat"><b>'+blocked+'</b><span>被阻塞</span></div>';
    $('projcount').textContent = s.projects.length;
    $('projects').innerHTML = s.projects.map(renderProject).join('') ||
      '<div class="muted">还没有项目。</div>';
    $('events').innerHTML = s.events.slice().reverse().map(function(e){
      var d = new Date(e.t*1000); var hh = ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2)+':'+('0'+d.getSeconds()).slice(-2);
      var cls = e.lvl==='error'?'errrow':(e.lvl==='warn'?'warnrow':'');
      return '<div class="evt '+cls+'">'+hh+' '+esc(e.msg)+'</div>';
    }).join('');
    $('conn').textContent = '已连接 · ' + new Date().toLocaleTimeString();
  }

  function renderProject(p){
    var total = p.tasks.length, done = p.counts.done||0;
    var pct = total? Math.round(done*100/total):0;
    var running = p.status==='running';
    var html = '<div style="margin-bottom:16px">'+
      '<div class="row" style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap">'+
      '<div style="flex:1;min-width:180px"><b>'+esc(p.title)+'</b> '+
      '<span class="pill">'+esc(p.id)+'</span> '+
      '<span class="pill">'+esc(pjName[p.status]||p.status)+'</span></div>'+
      (running?'<button data-act="pause" data-p="'+esc(p.id)+'">暂停</button>':
               '<button data-act="resume" data-p="'+esc(p.id)+'">继续</button>')+
      (p.tasks.some(function(t){return t.applied;})?
        '<button data-act="rollback" data-p="'+esc(p.id)+'">回滚最近合并</button>':'')+
      '<a class="tag" style="text-decoration:none" href="/api/export?project='+
        encodeURIComponent(p.id)+'">导出</a>'+
      '<button class="danger" data-act="delete" data-p="'+esc(p.id)+'">删除</button>'+
      '</div>'+
      '<div class="bar"><i style="width:'+pct+'%"></i></div>'+
      '<div class="sub" style="margin:4px 0 8px">'+done+'/'+total+' 完成 · 排队 '+(p.counts.queued||0)+
      ' · 冲突 '+(p.counts.conflict||0)+' · 阻塞 '+(p.counts.blocked||0)+
      ' · 失败 '+(p.counts.failed||0)+'</div>';
    p.tasks.forEach(function(t){
      var st = t.status, cls = stColor[st]||'';
      html += '<div class="task"><div class="row">'+
        '<span class="name">'+esc(t.title)+'</span>'+
        '<span class="tag '+cls+'">'+(stName[st]||st)+'</span>'+
        (t.node? '<span class="tag">'+esc(t.node)+'</span>':'')+
        (t.attempts>1? '<span class="tag">第'+t.attempts+'次</span>':'')+
        (t.applied? '<span class="tag">合并 '+t.applied+' 文件</span>':'')+
        ((st==='failed'||st==='cancelled')?
          '<button data-act="retry" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+'">重试</button>':'')+
        (t.applied? '<button data-act="rbtask" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+'">回滚</button>':'')+
        '<button data-act="log" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+'">日志</button>'+
        '</div>';
      if(t.blocked_by){ html += '<div class="sub warnrow">上游任务 '+esc(t.blocked_by)+
        ' 失败，本任务不会派发（先重试上游）</div>'; }
      if(t.feedback){
        html += '<details><summary class="sub" style="cursor:pointer">上次失败反馈（重试会带给 agent）</summary>'+
          '<div class="log">'+esc(t.feedback)+'</div></details>';
      }
      if(t.verify){
        html += '<div class="sub">验收：'+(t.verify.ok?'通过':'失败')+
          ' <span class="muted">'+esc(t.verify.cmd||'')+'</span>';
        if(!t.verify.ok){ html += '<button data-act="log" data-p="'+esc(p.id)+
            '" data-t="'+esc(t.id)+'">看完整输出</button>'; }
        html += '</div>';
      }
      if(t.log_tail && t.log_tail.length){
        html += '<div class="log">'+t.log_tail.map(esc).join('\n')+'</div>';
      }
      (t.conflicts||[]).forEach(function(c){
        html += '<div class="conf">⚠ '+esc(c.path)+' —— '+esc(c.reason||'冲突')+
          '<div><button data-act="diff" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+
          '" data-path="'+esc(c.path)+'">看差异</button>'+
          '<button data-act="theirs" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+
          '" data-path="'+esc(c.path)+'">用节点的版本</button>'+
          '<button data-act="ours" data-p="'+esc(p.id)+'" data-t="'+esc(t.id)+
          '" data-path="'+esc(c.path)+'">保留主工程</button></div></div>';
      });
      html += '</div>';
    });
    return html + '</div>';
  }

  function showLog(pid, tid){
    api('/api/tasklog?project='+encodeURIComponent(pid)+'&task='+encodeURIComponent(tid))
      .then(function(res){
        $('diffbox').style.display='block';
        $('difftitle').textContent = res.id+' · '+res.title+' · '+res.status;
        var out = [];
        (res.changes||[]).forEach(function(c){ out.push((c.deleted?'删除 ':'改动 ')+c.path); });
        (res.conflicts||[]).forEach(function(c){ out.push('冲突 '+c.path+'：'+(c.reason||'')); });
        if(res.verify){ out.push('验收 '+(res.verify.ok?'通过':'失败')+'：'+(res.verify.cmd||''));
          if(!res.verify.ok && res.verify.out){ out.push('--- 验收输出 ---', res.verify.out); } }
        out.push('--- 日志 ---');
        out = out.concat(res.log||[]);
        $('diffpre').textContent = out.join('\n') || '（没有日志）';
        $('diffbox').scrollIntoView({behavior:'smooth', block:'nearest'});
      }).catch(function(e){ alert('取日志失败：'+e.message); });
  }

  function showDiff(pid, tid, path){
    api('/api/diff?project='+encodeURIComponent(pid)+'&task='+encodeURIComponent(tid)+
        '&path='+encodeURIComponent(path)).then(function(res){
      $('diffbox').style.display='block';
      $('difftitle').textContent = path;
      $('diffpre').textContent = res.diff || '（没有差异）';
      $('diffbox').scrollIntoView({behavior:'smooth', block:'nearest'});
    }).catch(function(e){ alert('取差异失败：'+e.message); });
  }

  document.addEventListener('click', function(ev){
    var b = ev.target.closest ? ev.target.closest('button[data-act]') : null;
    if(!b) return;
    var act = b.getAttribute('data-act');
    var pid = b.getAttribute('data-p');
    var tid = b.getAttribute('data-t');
    var path = b.getAttribute('data-path');
    if(act==='diff'){ showDiff(pid, tid, path); return; }
    if(act==='log'){ showLog(pid, tid); return; }
    if(act==='delete'){
      if(!confirm('删除项目 '+pid+'？项目目录会一起删掉，已合并的产出也会没（建议先导出）。')) return;
    }
    var body, url;
    if(act==='pause'||act==='resume'){ body={project:pid, action:act}; url='/api/control'; }
    else if(act==='retry'){ body={project:pid, action:'retry', task:tid}; url='/api/control'; }
    else if(act==='rollback'){ body={project:pid}; url='/api/rollback'; }
    else if(act==='rbtask'){ body={project:pid, task:tid}; url='/api/rollback'; }
    else if(act==='delete'){ body={project:pid}; url='/api/delete'; }
    else { body={project:pid, task:tid, path:path, choice:act==='theirs'?'theirs':'ours'}; url='/api/resolve'; }
    b.disabled = true;
    api(url, {method:'POST', body: JSON.stringify(body)}).then(function(){
      b.disabled=false; refresh();
    }).catch(function(e){ b.disabled=false; alert('操作失败：'+e.message); });
  });

  $('nsubmit').addEventListener('click', function(){
    var title = $('ntitle').value.trim();
    var spec = $('nspec').value.trim();
    if(!title && !spec){ alert('至少写个名字或需求'); return; }
    this.disabled = true;
    var btn = this;
    api('/api/submit', {method:'POST', body: JSON.stringify({title:title||'未命名项目', spec:spec})})
      .then(function(){ $('ntitle').value=''; $('nspec').value=''; btn.disabled=false; refresh(); })
      .catch(function(e){ btn.disabled=false; alert('提交失败：'+e.message); });
  });

  $('tokreset').addEventListener('click', function(ev){ ev.preventDefault(); askedToken=false; needToken(); });
  $('diffclose').addEventListener('click', function(ev){ ev.preventDefault(); $('diffbox').style.display='none'; });

  function renderTokens(list){
    $('tokcount').textContent = list.length;
    if(!list.length){ $('tokens').innerHTML = '<span class="muted">还没有节点令牌。建议每台机器单独发一个。</span>'; return; }
    $('tokens').innerHTML = list.map(function(t){
      return '<div class="node"><span class="dot '+(t.revoked?'':'on')+'"></span>'+
        '<div style="flex:1"><b>'+esc(t.name)+'</b> <span class="sub">'+
        (t.revoked?'已吊销':'有效')+' · 节点 '+(t.node?esc(t.node):'首次连接时绑定')+
        ' · '+esc(t.hint)+'</span></div>'+
        (t.revoked?'':'<button class="danger" data-tok="'+esc(t.name)+'">吊销</button>')+
        '</div>';
    }).join('');
  }

  function refreshTokens(){
    api('/api/tokens').then(function(r){ renderTokens(r.tokens||[]); }).catch(function(){});
  }

  $('tokadd').addEventListener('click', function(){
    var name = $('tokname').value.trim();
    if(!name){ alert('给这个令牌起个名字（比如 手机A）'); return; }
    var btn = this; btn.disabled = true;
    api('/api/tokens', {method:'POST', body: JSON.stringify({action:'create', name:name})})
      .then(function(res){
        $('toknew').innerHTML = '新令牌 <b>'+esc(res.name)+'</b>（只显示这一次）：<br><code>'+
          esc(res.token)+'</code>';
        $('tokname').value=''; btn.disabled=false; refreshTokens();
      }).catch(function(e){ btn.disabled=false; alert('发放失败：'+e.message); });
  });

  document.addEventListener('click', function(ev){
    var b = ev.target.closest ? ev.target.closest('button[data-tok]') : null;
    if(!b) return;
    if(!confirm('吊销令牌 '+b.getAttribute('data-tok')+'？那台机器下次请求就失效。')) return;
    b.disabled = true;
    api('/api/tokens', {method:'POST', body: JSON.stringify({
      action:'revoke', key:b.getAttribute('data-tok')})})
      .then(function(){ refreshTokens(); })
      .catch(function(e){ b.disabled=false; alert('吊销失败：'+e.message); });
  });

  function refresh(){
    api('/api/state').then(render).catch(function(){
      $('conn').textContent = '未连接 / 需要令牌';
    });
    refreshTokens();
  }

  refresh();
  setInterval(refresh, 2000);
})();
</script>
</body>
</html>
"""
