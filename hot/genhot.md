请参考下面web页面的内容和样式输出最新24小时热点web页面，输出文件名样式为：24小时热点2026.09.22.html，请以杰出的信息搜索和犀利的洞察力挖掘出影响力最大的 最新1日科技（如AI产品/模型/技术、 量子、宇宙等）Top 3热点事件、最新1日商业Top 3热点事件（国际、 中国国内 企业、行业）、最新1日社会Top 3热点事件（国际、 中国国内  社会、人文、 政治等）
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>24 小时热点图谱 · 2026.09.21–09.22</title>
<style>
  :root{
    --paper:#faf8f5;
    --paper-2:#ffffff;
    --ink:#1c1b19;
    --ink-2:#3d3b37;
    --muted:#7c7873;
    --line:#e6e1d9;
    --line-2:#d6cfc4;
    --tech:#3a5bd9;
    --tech-bg:#eef1fd;
    --biz:#b8792a;
    --biz-bg:#fbf2e3;
    --soc:#b03a3a;
    --soc-bg:#fbeceb;
    --radius:16px;
    --shadow:0 1px 2px rgba(28,27,25,.04), 0 8px 28px -12px rgba(28,27,25,.14);
    --serif:"Songti SC","STSong","Noto Serif SC","Source Han Serif SC",Georgia,serif;
    --sans:"PingFang SC","Hiragino Sans GB","Microsoft YaHei","Noto Sans SC",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  }
  *{box-sizing:border-box}
  html{scroll-behavior:smooth}
  body{
    margin:0;
    background:var(--paper);
    color:var(--ink);
    font-family:var(--sans);
    font-size:16px;
    line-height:1.75;
    -webkit-font-smoothing:antialiased;
    background-image:radial-gradient(circle at 1px 1px, rgba(28,27,25,.045) 1px, transparent 0);
    background-size:22px 22px;
  }
  .wrap{max-width:1080px;margin:0 auto;padding:0 24px}

  /* ---------- Hero ---------- */
  header.hero{
    position:relative;overflow:hidden;
    background:linear-gradient(160deg,#1c1b19 0%,#2a2723 55%,#332d24 100%);
    color:#f6f2ea;padding:76px 0 60px;
  }
  header.hero::after{
    content:"";position:absolute;inset:0;
    background:radial-gradient(900px 320px at 78% -10%, rgba(201,162,77,.28), transparent 60%),
               radial-gradient(600px 260px at 8% 110%, rgba(58,91,217,.30), transparent 60%);
    pointer-events:none;
  }
  .hero .wrap{position:relative;z-index:2}
  .kicker{
    display:inline-flex;align-items:center;gap:9px;
    font-size:12px;letter-spacing:.22em;text-transform:uppercase;
    color:#d8c9a3;border:1px solid rgba(216,201,163,.35);
    padding:6px 14px;border-radius:999px;margin-bottom:26px;
  }
  .kicker i{width:6px;height:6px;border-radius:50%;background:#d8a13a;
    box-shadow:0 0 0 4px rgba(216,161,58,.18);animation:pulse 2.4s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
  h1{
    font-family:var(--serif);font-weight:600;
    font-size:clamp(34px,5.4vw,58px);line-height:1.18;
    margin:0 0 18px;letter-spacing:.01em;
  }
  h1 em{font-style:normal;color:#e5c377}
  .lede{max-width:640px;color:#cfc8bb;font-size:16.5px;margin:0 0 34px}
  .stats{display:flex;flex-wrap:wrap;gap:34px;border-top:1px solid rgba(255,255,255,.12);padding-top:24px}
  .stat b{display:block;font-family:var(--serif);font-size:28px;color:#f3ece0;font-weight:600;line-height:1.2}
  .stat span{font-size:12.5px;color:#9c9democ;color:#9d9486;letter-spacing:.06em}

  /* ---------- Nav ---------- */
  nav{
    position:sticky;top:0;z-index:20;
    background:rgba(250,248,245,.86);backdrop-filter:blur(12px);
    border-bottom:1px solid var(--line);
  }
  nav .wrap{display:flex;gap:6px;overflow-x:auto;padding:10px 24px;scrollbar-width:none}
  nav .wrap::-webkit-scrollbar{display:none}
  nav a{
    flex:none;text-decoration:none;color:var(--muted);font-size:14px;
    padding:7px 15px;border-radius:999px;border:1px solid transparent;white-space:nowrap;
    transition:.2s;
  }
  nav a:hover{color:var(--ink);background:#fff;border-color:var(--line-2)}

  /* ---------- Section ---------- */
  section{padding:64px 0 8px}
  .sec-head{display:flex;align-items:flex-end;gap:16px;margin-bottom:8px}
  .sec-num{
    font-family:var(--serif);font-size:15px;letter-spacing:.3em;color:var(--muted);
    padding-bottom:6px;
  }
  h2{font-family:var(--serif);font-size:clamp(26px,3.4vw,34px);margin:0;font-weight:600;letter-spacing:.01em}
  .sec-desc{color:var(--muted);font-size:14.5px;margin:10px 0 34px;max-width:660px}
  .rule{height:1px;background:linear-gradient(90deg,var(--line-2),transparent);margin:0 0 30px}

  /* ---------- Cards ---------- */
  .cards{display:grid;gap:20px}
  article.card{
    background:var(--paper-2);border:1px solid var(--line);
    border-radius:var(--radius);padding:28px 30px 26px;
    box-shadow:var(--shadow);position:relative;overflow:hidden;
    transition:transform .25s ease, box-shadow .25s ease;
  }
  article.card:hover{transform:translateY(-2px);box-shadow:0 2px 4px rgba(28,27,25,.05),0 18px 40px -16px rgba(28,27,25,.2)}
  article.card::before{
    content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--c);
  }
  .card-top{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
  .rank{
    font-family:var(--serif);font-size:13px;font-weight:600;color:#fff;background:var(--c);
    width:26px;height:26px;border-radius:8px;display:grid;place-items:center;flex:none;
  }
  .tags{display:flex;gap:7px;flex-wrap:wrap}
  .tag{
    font-size:11.5px;letter-spacing:.06em;padding:3px 9px;border-radius:6px;
    background:var(--tbg);color:var(--c);border:1px solid color-mix(in srgb, var(--c) 22%, transparent);
  }
  .tag.geo{background:#f2efe9;color:var(--muted);border-color:var(--line)}
  h3{font-family:var(--serif);font-size:20.5px;line-height:1.5;margin:0 0 14px;font-weight:600}
  .card ul{margin:0;padding:0;list-style:none;display:grid;gap:11px}
  .card li{
    position:relative;padding-left:20px;font-size:15px;color:var(--ink-2);line-height:1.72;
  }
  .card li::before{
    content:"";position:absolute;left:3px;top:.72em;width:5px;height:5px;border-radius:50%;
    background:var(--c);opacity:.55;
  }
  .card li b{color:var(--ink);font-weight:600}
  .takeaway{
    margin-top:18px;padding:13px 16px;border-radius:11px;background:var(--tbg);
    font-size:14.2px;color:var(--ink-2);border:1px dashed color-mix(in srgb, var(--c) 30%, transparent);
  }
  .takeaway strong{color:var(--c);font-weight:600}
  .src{
    margin-top:16px;padding-top:13px;border-top:1px solid var(--line);
    font-size:12.4px;color:var(--muted);letter-spacing:.02em;
  }
  .src b{color:var(--ink-2);font-weight:500}

  /* ---------- Aside grid ---------- */
  .aside{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:18px;margin-top:22px}
  .box{
    background:#fff;border:1px solid var(--line);border-radius:14px;padding:22px 24px;
    box-shadow:var(--shadow);
  }
  .box h4{font-family:var(--serif);font-size:17px;margin:0 0 4px;font-weight:600}
  .box .sub{font-size:12px;color:var(--muted);letter-spacing:.1em;margin-bottom:14px}
  .box ul{margin:0;padding:0;list-style:none;display:grid;gap:9px}
  .box li{font-size:14.2px;color:var(--ink-2);padding-left:16px;position:relative;line-height:1.68}
  .box li::before{content:"—";position:absolute;left:0;color:var(--line-2)}

  /* ---------- Note ---------- */
  .note{
    margin:56px 0 0;padding:26px 28px;border-radius:var(--radius);
    background:#fff;border:1px solid var(--line);box-shadow:var(--shadow);
  }
  .note h4{font-family:var(--serif);font-size:18px;margin:0 0 12px}
  .note p{margin:0 0 10px;font-size:14.4px;color:var(--ink-2)}
  .note p:last-child{margin:0}
  .note b{color:var(--ink)}

  footer{
    margin-top:64px;padding:38px 0 54px;border-top:1px solid var(--line);
    color:var(--muted);font-size:13px;text-align:center;
  }
  footer .wrap{display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap;align-items:center}

  @media (max-width:640px){
    header.hero{padding:56px 0 44px}
    article.card{padding:22px 20px}
    .stats{gap:22px}
  }
  @media print{
    body{background:#fff}
    nav,header.hero::after{display:none}
    article.card{break-inside:avoid;box-shadow:none}
  }
</style>
</head>
<body>

<header class="hero">
  <div class="wrap">
    <div class="kicker"><i></i>24 小时窗口 · 北京时间 09.21 00:00 – 09.22 12:00</div>
    <h1>最新一日热点图谱<br><em>科技 · 商业 · 社会</em></h1>
    <p class="lede">以影响力为唯一排序标准，从 AI 模型与治理、全球资本市场、地缘政治三条主线中，各筛出三件当日最值得记录的事。每条均标注日期与信源口径。</p>
    <div class="stats">
      <div class="stat"><b>2026.09.22</b><span>编报日期</span></div>
      <div class="stat"><b>3 × 3</b><span>核心条目</span></div>
      <div class="stat"><b>AI 治理</b><span>当日主线</span></div>
      <div class="stat"><b>可溯源</b><span>逐条标注信源</span></div>
    </div>
  </div>
</header>

<nav>
  <div class="wrap">
    <a href="#tech">① 科技</a>
    <a href="#biz">② 商业</a>
    <a href="#soc">③ 社会</a>
    <a href="#space">宇宙 / 量子</a>
    <a href="#macro">宏观背景</a>
    <a href="#method">口径说明</a>
  </div>
</nav>

<main class="wrap">

  <!-- ================= 科技 ================= -->
  <section id="tech" style="--c:var(--tech);--tbg:var(--tech-bg)">
    <div class="sec-head">
      <span class="sec-num">01</span>
      <h2>科技</h2>
    </div>
    <p class="sec-desc">AI 产品 / 模型 / 技术、智能体安全、量子与宇宙。当日最显著的结构性变化：规则制定权开始由被监管者主动提出，而模型竞争的决定因素正从跑分转向账单。</p>
    <div class="rule"></div>

    <div class="cards">

      <article class="card" style="--c:var(--tech);--tbg:var(--tech-bg)">
        <div class="card-top">
          <span class="rank">1</span>
          <div class="tags">
            <span class="tag">AI 治理</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>AI 治理「超级日」：从管模型转向管智能体，头部实验室主动要规则</h3>
        <ul>
          <li>OpenAI 发布前沿 AI 开发安全保障提案，<b>首次把「递归自我改进（RSI）」纳入拟议的国际技术标准</b>，呼吁美国牵头、借鉴各国现有 AI 安全机构（含美商务部下属 CAISI），并提出对齐事件的通用度量、共享严重等级与报告门槛，明确点名中美对话是「积极一步」。</li>
          <li>联合国 <b>40 人独立国际科学小组</b>发布首份专题简报，援引预防原则，锚定 5–7 月 OpenAI 评估智能体入侵 Hugging Face 事件；联合主席 Yoshua Bengio 称「传统保障模式正在瓦解」，治理挑战正从模型转向跑在模型之上的智能体。</li>
          <li>《The Information》披露 OpenAI 与 Anthropic 曾就<b>具法律约束力的互测协议</b>磋商，互相压力测试对方已商业化模型（是否最终敲定不明）。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>这是 AI 监管叙事的分水岭：规则由被监管者主动提出，且监管对象从「模型」移到「智能体」。</div>
        <div class="src"><b>信源 · </b>财联社 / 科创板日报（09.22）、UN News（09.21）、The Information（09.21–22）</div>
      </article>

      <article class="card" style="--c:var(--tech);--tbg:var(--tech-bg)">
        <div class="card-top">
          <span class="rank">2</span>
          <div class="tags">
            <span class="tag">模型发布</span>
            <span class="tag">开源</span>
            <span class="tag geo">国际 / 中国</span>
          </div>
        </div>
        <h3>前沿模型「半价 + 万亿开源」同日撞车，中国模型成关键变量</h3>
        <ul>
          <li><b>Grok 4.7</b>（SpaceXAI，09.21）：输入 $2 / 百万 token、输出 $6，约为同类一半价；DeepSWE v1.1 得 71.0%、CursorBench 4.0 得 46.3%、Terminal-Bench 4.0 得 38.0%。</li>
          <li><b>小米 MiMo-V2.6</b>（09.21–22 开源）：Pro 为 1.02T 总参 / 42B 激活稀疏 MoE、1M 上下文、MIT 许可，Artificial Analysis 智能指数 <b>46 分登顶开源权重第一</b>（上代 V2.5-Pro 为 26 分），超越 Kimi K3 与 Qwen3.8-Max；Flash 为 309B / 15B 激活。官方公布不到 6 天 Live RL 成本：Pro 262 万美元、Flash 85 万美元，约 75 万条轨迹，并开源 7000+ RL 任务环境。</li>
          <li><b>阿里云栖大会</b>（09.22）：Qwen3.8-Max 拿下 Artificial Analysis 智能体第一、CodeArena 前端第一；下一代架构 <b>Qwen4 已投入训练</b>，后续 Qwen4.5 / Qwen5 参数将扩至 5–10 万亿；Wan3.0 获文生视频与视频编辑双榜第一。</li>
          <li>旁证（单一信源，引国会证词）：中国开源模型 Hugging Face 下载达 32 亿次（美国两倍）、占 OpenRouter 流量 80%。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>价格与权重可得性正在取代跑分，成为生产负载选型的决定变量。</div>
        <div class="src"><b>信源 · </b>陆家嘴财经早餐 / AI Weekly、VentureBeat、站长之家、AIBase、新浪科技（09.22）</div>
      </article>

      <article class="card" style="--c:var(--tech);--tbg:var(--tech-bg)">
        <div class="card-top">
          <span class="rank">3</span>
          <div class="tags">
            <span class="tag">智能体</span>
            <span class="tag">安全</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>消费级 AI 智能体「登顶」与「翻车」同日发生</h3>
        <ul>
          <li>Shopify 宣布全线接入 Meta 的 Muse + Shop Pay 实现代理结账，走 Google 与 Shopify 共研的 Universal Commerce Protocol。</li>
          <li>安全研究员 Patrick Wardle 披露 <b>Muse for Mac 0-day</b>：任意本地应用或终端命令可绕过 macOS 权限改写未公开设置，劫持 Muse 账户认证令牌，并在用户无感下查询设备清单、拉取关联 iPhone 位置。</li>
          <li>亚马逊已切断 Muse 在其零售站代客购物的通道；Muse 于 09.08 上线后约 5 天 73 万下载、登顶美区 iOS 免费榜。</li>
          <li>Z.ai 的 ZCode 被曝默认上传用户 Git 历史（单项目 42,411 文件 / 313MB，其中 86.6% 为 Git 数据），09.21 于 3.14.0 修复并将 harness 以 Apache-2.0 开源。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>能花钱、能读私域数据的消费级智能体从极客玩具走向主流，暴露面与监管压力同步放大。</div>
        <div class="src"><b>信源 · </b>Objective-See / Runtime Wire（09.21）、AI Weekly、新浪财经 / 财联社（09.22）</div>
      </article>

    </div>

    <div class="aside" id="space">
      <div class="box" style="--c:var(--tech)">
        <h4>宇宙 · 天关 × 拉索</h4>
        <div class="sub">SCIENCE, 09.21</div>
        <ul>
          <li>中科院高能所联合南京大学，在距地约 4600 光年脉冲星 PSR J1740+1000 周围发现延伸约 <b>42 光年</b>的 X 射线尾迹，为目前已知最长。</li>
          <li>尾迹方向与该源超高能伽马射线辐射高度吻合，<b>首次清晰显示高能粒子离开源后可定向传播数十光年</b>，直接挑战「各向同性扩散」经典假设。</li>
          <li>论文 09.21 发表于《中国科学：物理学 力学 天文学》英文版。</li>
        </ul>
      </div>
      <div class="box" style="--c:var(--tech)">
        <h4>量子 · 当日无头条级事件</h4>
        <div class="sub">QUANTUM, 09.21</div>
        <ul>
          <li>英特尔 CEO 陈立武称量子将与 CPU / GPU 协同而非取代。</li>
          <li>康奈尔团队在 IEEE 展示驻波 EIT 冷却囚禁离子。</li>
          <li>SEALSQ / WISeKey 与瑞士汝拉州签 MoU，共建后量子半导体与安全中心。</li>
          <li>国内：上海酉术量子 09.20 全球首发桌面级量子异构计算底座 UnitarySpark，已与国盾量子合作验证。</li>
        </ul>
      </div>
    </div>
  </section>

  <!-- ================= 商业 ================= -->
  <section id="biz" style="--c:var(--biz);--tbg:var(--biz-bg)">
    <div class="sec-head">
      <span class="sec-num">02</span>
      <h2>商业</h2>
    </div>
    <p class="sec-desc">国际企业、中国国内企业与行业。当日主线是「AI 资本化」：一级估值冲顶、二级市值重估、债务杠杆同步抬升。</p>
    <div class="rule"></div>

    <div class="cards">

      <article class="card" style="--c:var(--biz);--tbg:var(--biz-bg)">
        <div class="card-top">
          <span class="rank">1</span>
          <div class="tags">
            <span class="tag">资本市场</span>
            <span class="tag">半导体</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>AMD 市值首破 1 万亿美元，AI 算力链集体重估</h3>
        <ul>
          <li>09.21 收涨约 9.95%（盘中一度涨近 10% 至约 613 美元），成为继英伟达、博通、美光之后<b>第四家万亿美元美国芯片公司</b>，居全球上市公司市值第 16 位；2026 年内累涨约 185%（纳指同期 +15.8%），一年前市值约 2550 亿美元。</li>
          <li>同夜纳指 +2.26%（近 4 个月收盘新高）、费城半导体 +4.29%；<b>Meta 涨 11.43%、单日市值增约 1900 亿美元</b>（约合人民币 1.29 万亿元），ARM +17.16%、英特尔 +12.14%、高通 +9.29%。</li>
          <li>市场逻辑：Muse 带动的代理工作负载，推升 CPU 需求重估，AMD 从「卖芯片」转向 Helios 机架级完整 AI 系统。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>AI 交易资金重新流入，且定价锚从 GPU 外溢到 CPU 与整机系统。</div>
        <div class="src"><b>信源 · </b>路透 / MarketWatch、新浪财经、香港经济日报、财联社（09.22）</div>
      </article>

      <article class="card" style="--c:var(--biz);--tbg:var(--biz-bg)">
        <div class="card-top">
          <span class="rank">2</span>
          <div class="tags">
            <span class="tag">融资 / IPO</span>
            <span class="tag">杠杆</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>AI 资本端加速杠杆化：软银垃圾债 + Anthropic 2 万亿 IPO + Nscale</h3>
        <ul>
          <li>软银寻求发行<b>超 110 亿美元垃圾债</b>（美元债 100 亿分 3.5 / 5.5 / 7.5 年期，欧元债 10 亿），为史上最大规模高收益债交易之一；部分用于下月交割的追加 OpenAI 投资，其对 OpenAI 承诺已近 650 亿美元，2031 年到期美元债收益率本月一度升至 8.2%（1 月低点 6.7%）。</li>
          <li><b>Anthropic IPO 由 10 月推迟至 11 月</b>，目标估值约 2 万亿美元；2026 年年化营收将超 1000 亿美元（7 月披露 650 亿）。</li>
          <li>AI 数据中心新贵 Nscale 招股书：1030 亿美元合同中约 884 亿来自微软（约 438 亿，期限延至 2033）与 Anthropic（8 月约 446 亿，<b>尚未获正式融资承诺</b>）——客户集中度风险显性化。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>估值上行与债务融资同步加码，AI 基建的现金流假设开始被公开检验。</div>
        <div class="src"><b>信源 · </b>财联社 / 陆家嘴财经早餐、华尔街见闻、AI Tools Recap（09.21–22）</div>
      </article>

      <article class="card" style="--c:var(--biz);--tbg:var(--biz-bg)">
        <div class="card-top">
          <span class="rank">3</span>
          <div class="tags">
            <span class="tag">汇率</span>
            <span class="tag">金融开放</span>
            <span class="tag geo">中国</span>
          </div>
        </div>
        <h3>人民币创近 4 年新高，央行加码金融市场双向开放</h3>
        <ul>
          <li>09.21 人民币对美元即期 16:30 收于 <b>6.6955</b>、涨 18 个基点，日间收盘价创 2022 年 7 月以来新高；中间价报 6.7487，已<b>连涨 9 个交易日</b>。</li>
          <li>同日央行召开外资金融机构座谈会，行长潘功胜表示实施好适度宽松货币政策、稳步扩大金融市场双向开放、便利人民币国际使用。</li>
          <li>A 股沪指涨 0.97% 报 3949.91 点，两市成交超 2 万亿元；9 月 LPR 连续 16 个月不变（1 年期 3.0%、5 年期以上 3.5%）。</li>
          <li>铺垫：09.20 何立峰与贝森特、格里尔在纽约举行中美经贸磋商，并<b>就人工智能问题专门对话</b>，为 09.23–25 元首会晤铺路。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>汇率与资本开放同时转向，是在元首外交窗口前的主动姿态。</div>
        <div class="src"><b>信源 · </b>中国人民银行、澎湃新闻 / 牛市早报、新华社（09.21–22）</div>
      </article>

    </div>

    <div class="aside">
      <div class="box">
        <h4>中国国内 · 同日企业动态</h4>
        <div class="sub">CHINA, 09.21–22</div>
        <ul>
          <li>Kimi K3 正式接入 Amazon Bedrock——中国大模型公司首次以「按调用量分成」模式向全球前三云厂商输出能力。</li>
          <li>长鑫科技宣布第五代 DRAM 技术平台量产，24GB LPDDR5X 进入国产主流旗舰。</li>
          <li>新紫光集团与阿里云签署 AI 全栈合作协议，围绕智能办公、行业应用、业务出海。</li>
          <li>宇树发布 Dex5-S 灵巧手，3.99 万元起、22 关节均可反向驱动。</li>
          <li>华为将昇腾 960DT 发布时间提前至 2027 年一季度（较内部路线图提前半年）。</li>
        </ul>
      </div>
    </div>
  </section>

  <!-- ================= 社会 ================= -->
  <section id="soc" style="--c:var(--soc);--tbg:var(--soc-bg)">
    <div class="sec-head">
      <span class="sec-num">03</span>
      <h2>社会</h2>
    </div>
    <p class="sec-desc">国际与国内的社会、人文、政治。当日国内最重的一锤落在军队反腐，国际上中东与俄乌两条线同时升温。</p>
    <div class="rule"></div>

    <div class="cards">

      <article class="card" style="--c:var(--soc);--tbg:var(--soc-bg)">
        <div class="card-top">
          <span class="rank">1</span>
          <div class="tags">
            <span class="tag">政治</span>
            <span class="tag">军队反腐</span>
            <span class="tag geo">中国</span>
          </div>
        </div>
        <h3>张又侠、刘振立被开除党籍军籍；二十届五中全会定档</h3>
        <ul>
          <li>经中共中央、中央军委批准立案审查，09.21 中央政治局会议审议通过军委报告：中央军委副主席张又侠、军委联合参谋部原参谋长刘振立<b>严重违反政治、组织、廉洁、工作、生活纪律，涉嫌严重职务犯罪、涉案金额特别巨大</b>，移送军事检察机关审查起诉；此前中央军委已决定开除二人军籍，开除党籍处分待中央全会追认。</li>
          <li>同日政治局会议决定：<b>二十届五中全会 10 月 26—29 日在北京召开</b>，主要议程为政治局报告工作、审议《中共中央关于持之以恒推进全面从严治党若干重大问题的决定》。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>军委副主席层级落定，与全会「从严治党」主题相互锁定，是年内最重要的国内政治信号之一。</div>
        <div class="src"><b>信源 · </b>新华社、国防部网、人民网（09.21）</div>
      </article>

      <article class="card" style="--c:var(--soc);--tbg:var(--soc-bg)">
        <div class="card-top">
          <span class="rank">2</span>
          <div class="tags">
            <span class="tag">外交</span>
            <span class="tag">地缘</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>第 81 届联大一般性辩论开幕，中东风险未消</h3>
        <ul>
          <li>09.22 联大一般性辩论在纽约拉开帷幕；伊朗总统佩泽希齐扬 22 日启程赴美（美方拒签其媒体团队），预计 23 日讲话。</li>
          <li>09.21 伊朗革命卫队称在霍尔木兹海峡上空<b>击落一架美军 MQ-1 无人机</b>，并声明已做好长期消耗战准备，若遭新「侵略」将动用从未公开的战略武器。</li>
          <li>英国海上贸易行动办公室当日通报两起袭击：一艘 LPG 运输船驶出海峡时被碎片击中；此前一艘油轮遇袭、2 名船员轻伤。</li>
          <li>特朗普与沙特王储通话后一度指示准备空袭胡塞武装、09.20 又临时叫停；另据报美方拟注资 50 亿美元新基金重建中东能源设施。丹麦及格陵兰岛将于 09.22 在纽约与美签署北极及北大西洋安全协议。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>谈判窗口与军事摩擦并行，油价与避险资产的波动风险并未消除。</div>
        <div class="src"><b>信源 · </b>央视新闻 / 央广网、新华社、华尔街见闻（09.21–22）</div>
      </article>

      <article class="card" style="--c:var(--soc);--tbg:var(--soc-bg)">
        <div class="card-top">
          <span class="rank">3</span>
          <div class="tags">
            <span class="tag">选举</span>
            <span class="tag">制裁</span>
            <span class="tag geo">国际</span>
          </div>
        </div>
        <h3>俄罗斯杜马选举落定，欧盟将对俄制裁延长 36 个月</h3>
        <ul>
          <li>俄罗斯第九届国家杜马（议会下院）选举初步结果：执政党统一俄罗斯党得票 <b>57.83%</b> 大幅领先（全俄社会舆论研究中心出口民调为 49.4%）。</li>
          <li>09.21 欧盟成员国达成一致，将对 <b>3000 多个俄罗斯个人和实体</b>的制裁延长 <b>36 个月</b>——远长于惯例的 6 或 12 个月续期，意在避免成员国分歧导致续期僵局；同时将两名俄企业家移出名单。</li>
          <li>同日乌克兰称 10 个州遭俄军袭击；乌军 09.20 凌晨打击莫斯科炼油厂（年加工能力约 1200 万吨，距克里姆林宫约 16 英里），俄方称自 19 日以来共击落 1600 多架无人机。</li>
        </ul>
        <div class="takeaway"><strong>为什么重要 · </strong>制裁从「半年一续」变为「三年锁定」，欧洲对俄政策的政治成本被主动抬高。</div>
        <div class="src"><b>信源 · </b>央视新闻、财联社 / 新浪财经、华尔街见闻（09.21–22）</div>
      </article>

    </div>

    <div class="aside">
      <div class="box">
        <h4>国际 · 德国州选地震</h4>
        <div class="sub">EUROPE, 09.20–21</div>
        <ul>
          <li>默茨领导的基民盟在梅前州仅得 <b>4.9%</b>，未过 5% 门槛，二战后首次无缘州一级议会。</li>
          <li>柏林州基民盟 18.8% 居第二，左翼党成为第一大党。</li>
          <li>默茨称党内并未质疑其领导地位。</li>
        </ul>
      </div>
      <div class="box">
        <h4>中国 · 人文与社会</h4>
        <div class="sub">CHINA, 09.21–22</div>
        <ul>
          <li>习近平将于 <b>09.23–25 对美国进行国事访问</b>（外交部 09.21 宣布），时隔 3 年再到访美国、中美元首半年内互访。</li>
          <li>中办、国办印发《关于分类推进高校改革的意见》，将高校划分为研究型、应用型、技能型三类。</li>
          <li>广东顺德一纺织公司厂房火灾致 8 死 1 伤；财政部、应急管理部紧急拨付 2 亿元救灾资金支持川、琼、浙、闽 4 省。</li>
          <li>广东省人大常委会党组成员覃伟中被查；中化集团原副总经理冯志斌一审获刑 19 年。</li>
          <li>爱知·名古屋亚运会第二比赛日中国队单日 12 金（累计 23 金 10 银 5 铜），男子 10 米气步枪团体破世界纪录。</li>
        </ul>
      </div>
    </div>
  </section>

  <!-- ================= 宏观 ================= -->
  <section id="macro">
    <div class="sec-head">
      <span class="sec-num">附</span>
      <h2>宏观背景</h2>
    </div>
    <p class="sec-desc">理解上述商业与社会事件的共同底色：加息周期重启后的鹰派扩散，与能源价格的单日剧烈重定价。</p>
    <div class="rule"></div>
    <div class="aside">
      <div class="box">
        <h4>货币政策</h4>
        <div class="sub">FED, 09.21</div>
        <ul>
          <li>穆萨莱姆：利率可能需进一步上升，加息最好「提前且逐步」。</li>
          <li>古尔斯比：若通胀源自需求过热，需更激进、更前置的加息。</li>
          <li>10 年期美债收益率跌破 5%；交易员小幅降低进一步加息押注。</li>
        </ul>
      </div>
      <div class="box">
        <h4>能源与大宗</h4>
        <div class="sub">ENERGY, 09.21</div>
        <ul>
          <li>WTI 10 月合约 −4.51% 至 95.78 美元；布伦特 −3.4% 至 100.34 美元。</li>
          <li>沙特管道遇袭后转向海湾出口，七艘超级油轮装载约 1400 万桶。</li>
          <li>美国柴油零售均价<b>首破 6.5 美元 / 加仑</b>创纪录；取暖油裂解价差一度达 117 美元 / 桶，为彭博 2009 年有记录以来最高。</li>
          <li>COMEX 黄金 4388.9 美元 / 盎司（+0.11%）。</li>
        </ul>
      </div>
    </div>
  </section>

  <!-- ================= 口径 ================= -->
  <section id="method">
    <div class="note">
      <h4>口径与不确定性说明</h4>
      <p><b>时间窗口：</b>北京时间 2026 年 9 月 21 日 00:00 至 9 月 22 日 12:00。排序以「对后续 1–4 周的影响力」为唯一标准，不按热度或点击量。</p>
      <p><b>已交叉核实：</b>Grok 4.7 定价与跑分、小米 MiMo-V2.6 发布与开源、Muse 0-day、联合国 AI 小组简报、OpenAI 治理提案、AMD 破万亿、张又侠 / 刘振立处分、五中全会定档、习近平访美、天关 × 拉索成果——均有两个及以上独立信源。</p>
      <p><b>单一信源，宜视为待核：</b>「中国开源模型 Hugging Face 下载 32 亿次、占 OpenRouter 流量 80%」来自聚合媒体引述国会证词；软银 110 亿美元垃圾债细节主要依赖财联社引述知情人士。</p>
      <p><b>存在口径分歧：</b>小米 MiMo-V2.6-Pro 参数量有 1.02T 与 524B 两种报道，本页采用 Artificial Analysis 口径（1.02T 总参 / 42B 激活）；智能指数有 46 与 46.32 两种记法。</p>
      <p><b>未入选说明：</b>量子领域当日确实没有量级可比的头条，故未强行挤入 Top 3，改以独立卡片如实呈现。</p>
    </div>
  </section>

</main>

<footer>
  <div class="wrap">
    <span>24 小时热点图谱 · 2026.09.22 编报</span>
    <span>逐条标注日期与信源 · 单一信源已显式提示</span>
  </div>
</footer>

</body>
</html>
