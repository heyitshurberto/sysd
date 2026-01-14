import fs from 'fs';
import fetch from 'node-fetch';
import { createRequire } from 'module';
import { execSync } from 'child_process';
import crypto from 'crypto';
import express from 'express';

// Load environment variables from .env file
if (fs.existsSync('.env')) {
  const envContent = fs.readFileSync('.env', 'utf8');
  envContent.split('\n').forEach(line => {
    const [key, value] = line.split('=');
    if (key && value) {
      process.env[key.trim()] = value.trim();
    }
  });
}

const CONFIG = {
  // Alert filtering criteria
  FILE_TIME: 1, // Minutes retro to fetch filings
  MIN_ALERT_VOLUME: 50000, // Min volume threshold
  MAX_FLOAT: 100000000, // Max float size
  MAX_SO_RATIO: 125.0,  // Max short interest ratio - Increased from 80% to 100%
  ALLOWED_COUNTRIES: ['israel', 'dubai', 'japan', 'china', 'hong kong', 'brazil', 'cayman islands', 'virgin islands', 'singapore', 'canada', 'ireland', 'california', 'delaware', 'massachusetts', 'texas', 'australia'], // Allowed incorporation/located countries
  // Enable optimizations for Raspberry Pi devices
  PI_MODE: true,             // Enable Pi optimizations          
  REFRESH_PEAK: 1,       // 10s during trading hours (7am-10am ET)
  REFRESH_NORMAL: 30000,     // 30s during trading hours (3:30am-6pm ET)
  REFRESH_NIGHT: 300000,     // 5m outside trading hours (conserve power)
  REFRESH_WEEKEND: 600000,   // 10m on weekends (very low activity)
  YAHOO_TIMEOUT: 10000,      // Reduced from 10s for Pi performance
  SEC_RATE_LIMIT: 5000,      // Minimum 5ms between SEC requests
  SEC_FETCH_TIMEOUT: 10000,   // Increased to 10s for large SEC filings (was 5s causing timeouts)
  MAX_COMBINED_SIZE: 100000, // Reduced from 150k for Pi RAM
  MAX_RETRY_ATTEMPTS: 7,     // Reduced from 7 for Pi resources
  // Log files
  ALERTS_FILE: 'logs/alert.json', // File to store recent alerts
  STOCKS_FILE: 'logs/stocks.json', // File to store all alerts
  PERFORMANCE_FILE: 'logs/quote.json', // File to store performance data
  CSV_FILE: 'logs/backtest.csv', // File to store CSV export of all alerts
  // GitHub & Webhook settings
  GITHUB_REPO_PATH: process.env.GITHUB_REPO_PATH || '/home/user/Documents/sysd', // Local path to GitHub repo
  GITHUB_USERNAME: process.env.GITHUB_USERNAME || 'your-github-username', // GitHub username
  GITHUB_REPO_NAME: process.env.GITHUB_REPO_NAME || 'your-repo-name', // GitHub repo name
  GITHUB_DOMAIN: process.env.GITHUB_DOMAIN || 'your-domain.com', // GitHub Pages domain
  PERSONAL_WEBHOOK_URL: process.env.DISCORD_WEBHOOK || '' // Personal Discord webhook URL
};

const originalLog = console.log;
const originalWarn = console.warn;
const originalError = console.error;
const originalWrite = process.stdout.write;
const suppressPatterns = [
  'Fetching crumb', 'We expected', "We'll try", 'Success. Cookie', 'New crumb',
  'guce.yahoo.com', 'consent.yahoo.com', 'query1.finance.yahoo.com', 'collectConsent', 'copyConsent',
  'redirect to guce', 'getcrumb', '/quote/AAPL',
  'yahoo-finance2', 'v2 is no longer maintained nor supported', 'Please migrate to v3',
  'Circuit open', 'returning cached quote', 'Opening circuit', 'attempt', 'Unexpected token',
  'Using cached quote', 'Quote fetch failed', 'Failed to fetch quote'
];
const isSuppressed = (msg) => {
  if (!msg) return false;
  const str = msg.toString ? msg.toString() : String(msg);
  if (str.startsWith('fetch ')) return true;
  return suppressPatterns.some(pattern => str.includes(pattern));
};
console.log = (...args) => {
  const msg = args[0]?.toString() || '';
  if (!isSuppressed(msg)) originalLog(...args);
};
console.warn = (...args) => {
  const msg = args[0]?.toString() || '';
  if (!isSuppressed(msg)) originalWarn(...args);
};
console.error = (...args) => {
  const msg = args[0]?.toString() || '';
  if (!isSuppressed(msg)) originalError(...args);
};
process.stdout.write = function(str) {
  if (!isSuppressed(str)) return originalWrite.call(process.stdout, str);
  return true;
};

const require = createRequire(import.meta.url);
const YahooFinance = require('yahoo-finance2').default;
const yahooFinance = new YahooFinance();

process.env.DEBUG = '';

const rateLimit = {
  lastRequest: 0,
  minInterval: CONFIG.SEC_RATE_LIMIT,
  async wait() {
    const now = Date.now();
    const waitTime = this.minInterval - (now - this.lastRequest);
    if (waitTime > 0) await wait(waitTime);
    this.lastRequest = Date.now();
  }
};

// Custodian Bank Detection - verify actual ADR structures with word boundaries
// Returns object with custodian name or false if not found
// Maps real filing language: "JPMorgan" as custodian, Form F-6 sections, ADR program language
const detectCustodianBanks = (text) => {
  if (!text) return false;
  
  const lowerText = text.toLowerCase();
  
  // Pattern 1: Custodian bank designations (word boundaries prevent false matches)
  // Looks for "jpmorgan" or "j.p. morgan" as custodian or depositary
  const custodianPatterns = [
    { pattern: /\b(jpmorgan|j\.p\.\s*morgan|jp\s*morgan)\s*(chase|bank|services|as\s*custodian|as\s*depositary)/, name: 'JPMorgan Chase' },
    { pattern: /\b(citibank|citicorp|citi\s*bank)\s*(as\s*custodian|as\s*depositary|bank|n\.a\.)/, name: 'Citibank' },
    { pattern: /\bcitigroup\s*(inc|bank)/, name: 'Citigroup' },
    { pattern: /\bbny\s*mellon|bny\s*mellon|bank\s*of\s*new\s*york\s*mellon/, name: 'BNY Mellon' },
    { pattern: /\bdeutsche\s*bank/, name: 'Deutsche Bank' },
    { pattern: /\bstate\s*street\s*(bank|corporation)/, name: 'State Street' },
    { pattern: /\bwilmington\s*trust/, name: 'Wilmington Trust' }
  ];
  
  // Check custodian bank patterns with word boundaries
  for (const { pattern, name } of custodianPatterns) {
    if (pattern.test(lowerText)) {
      return { custodian: name, verified: true };
    }
  }
  
  // Pattern 2: Form F-6 filing (official ADR registration)
  // F-6 is specifically for ADR registration with SEC
  if (/form\s*f-6|f-6\s*registration|depositary\s*form\s*f-6/.test(lowerText)) {
    return { custodian: 'Form F-6 ADR', verified: true };
  }
  
  // Pattern 3: ADR program language in actual context
  // "American Depositary Receipt program" or "ADR program established"
  if (/american\s*depositary\s*receipt\s*(program|shares?|securities?)|adr\s*(program|shares?|securities?)\s*(for|of|issued)/i.test(lowerText)) {
    return { custodian: 'ADR Program', verified: true };
  }
  
  // Pattern 4: Foreign private issuer + depositary language together
  // Both must appear - reduces false positives
  const hasForeignPrivateIssuer = /foreign\s*private\s*issuer/.test(lowerText);
  const hasDepositaryRef = /depositary|depositary\s*(shares?|bank|agreement)/.test(lowerText);
  if (hasForeignPrivateIssuer && hasDepositaryRef) {
    return { custodian: 'Foreign Depositary', verified: true };
  }
  
  return false;
};

// S/O Bonus Multiplier - tighter float = stronger move potential
// High S/O (tight float) = 1.0-1.1x bonus based on float percentage
const getSOBonus = (float, sharesOutstanding) => {
  if (!float || !sharesOutstanding || isNaN(float) || isNaN(sharesOutstanding)) return 1.0;
  const soPercent = (float / sharesOutstanding) * 100;
  
  if (soPercent >= 50) return 1.1;     // 10% bonus for very tight float (50%+)
  else if (soPercent >= 30) return 1.08; // 8% bonus for tight (30-50%)
  else if (soPercent >= 15) return 1.05; // 5% bonus for moderate (15-30%)
  else return 1.0;                       // No bonus for loose float (<15%)
};

// Signal Score Calculator - Probabilistic weighted model
// Volume (50%) + Float (25%) + S/O (25%) with signal multipliers
const calculatesignalScore = (float, sharesOutstanding, volume, avgVolume, signalCategories = [], incorporated = null, located = null, filingText = '', companyName = '') => {
  // Float Score - micro-cap advantage but tempered (smaller is slightly better)
  let floatScore = 0.3;
  const floatMillion = float / 1000000;
  if (floatMillion < 1) floatScore = 0.45;
  else if (floatMillion < 2.5) floatScore = 0.42;
  else if (floatMillion < 5) floatScore = 0.40;
  else if (floatMillion < 10) floatScore = 0.38;
  else if (floatMillion < 25) floatScore = 0.35;
  else if (floatMillion < 50) floatScore = 0.32;
  else if (floatMillion < 100) floatScore = 0.30;
  else floatScore = 0.25;
  
  // S/O Score - lower baseline (float/shares ratio is just one factor)
  let soScore = 0.35;
  const numFloat = parseFloat(float) || 1;
  const numShares = parseFloat(sharesOutstanding) || 1;
  const soPercent = numShares > 0 ? (numFloat / numShares) * 100 : 50;

  if (soPercent < 5) soScore = 0.50;
  else if (soPercent < 15) soScore = 0.48;
  else if (soPercent < 30) soScore = 0.45;
  else if (soPercent < 50) soScore = 0.42;
  else if (soPercent < 75) soScore = 0.40;
  else soScore = 0.35;

  // Volume Score - MUST show real spike to matter (2x+ average = 0.6, not 1.0)
  let volumeScore = 0.25;
  const volumeRatio = avgVolume > 0 ? volume / avgVolume : 0.5;
  if (volumeRatio >= 3.0) volumeScore = 0.65;  // Major spike (3x+)
  else if (volumeRatio >= 2.0) volumeScore = 0.55;  // Significant (2x+)
  else if (volumeRatio >= 1.5) volumeScore = 0.45;  // Moderate
  else if (volumeRatio >= 1.0) volumeScore = 0.35;  // Slight increase
  else if (volumeRatio >= 0.8) volumeScore = 0.25;  // Below average
  else volumeScore = 0.15;

  // Signal Strength Multiplier (conservative - only major catalysts get real boost)
  let signalMultiplier = 1.0;
  const deathSpiralCats = ['Going Concern', 'Accounting Restatement', 'Bankruptcy Filing', 'Dilution', 'Reverse Split', 'Compliance Issue', 'Insider Selling'];
  const hasDeathSpiral = signalCategories?.some(cat => deathSpiralCats.includes(cat));
  const hasSqueeze = signalCategories?.some(cat => cat === 'Reverse Split');
  
  if (hasDeathSpiral) signalMultiplier = 1.15;  // 15% boost, not 30%
  else if (hasSqueeze) signalMultiplier = 1.10;  // 10% boost, not 20%
  else signalMultiplier = 1.0;

  // ADR Detection - verify actual custodian banks + incorporated != located structure
  // Checks for custodian keywords OR ADR structure (not just country mismatch)
  let adrMultiplier = 1.0;
  let isCustodianVerified = false;
  let custodianName = null;
  
  // MAXIMUM RED FLAG: "Not Applicable" = Ghost company (no origin, pure shell)
  if ((incorporated && incorporated.includes('Not Applicable')) || (located && located.includes('Not Applicable')) || (companyName && companyName.includes('Not Applicable'))) {
    adrMultiplier = 1.2;  //  Absolute scam signal
    isCustodianVerified = false;
    custodianName = 'Ghost Company (N/A)';
  }
  // First check for actual custodian bank keywords in filing text
  else if ((custodianResult = detectCustodianBanks(filingText))) {
    adrMultiplier = 1.3;  // 30% boost for verified custodian-controlled ADRs
    isCustodianVerified = true;
    custodianName = custodianResult.custodian;
  }
  // Fallback: ADR structure detection (incorporated != located) if no custodian bank found
  else if (incorporated && located && incorporated.toLowerCase() !== located.toLowerCase()) {
    // Only apply reduced boost (1.15x) since not verified via text scan
    adrMultiplier = 1.15; // 15% boost for ADR-like structure without custodian verification
    isCustodianVerified = false;
    custodianName = 'Structure Only';
  }
  
  // S/O Bonus - float tightness matters differently based on custodian control
  let soBonus = 1.0;
  if (numShares > 0) {
    const soPercent = (numFloat / numShares) * 100;
    const isADRStructure = adrMultiplier > 1.0;
    
    // ADR (custodian-controlled): tight float = suppressed supply = pump potential
    // Higher float ratio = tighter float = better for ADR
    if (isADRStructure) {
      if (soPercent >= 100) soBonus = 1.2;   // Extreme tight (100%+)
      else if (soPercent >= 50) soBonus = 1.15; // Very tight (50-100%)
      else if (soPercent >= 30) soBonus = 1.1;  // Tight (30-50%)
      else if (soPercent >= 15) soBonus = 1.05; // Moderate (15-30%)
    } else {
      // Non-ADR: loose float = better momentum without custodian control
      // Lower float ratio = looser float = better for regular stocks
      if (soPercent < 5) soBonus = 1.2;      // Extremely loose (<5%)
      else if (soPercent < 15) soBonus = 1.15; // Very loose (5-15%)
      else if (soPercent < 30) soBonus = 1.1;  // Loose (15-30%)
      else if (soPercent < 50) soBonus = 1.05; // Moderate (30-50%)
    }
  }

  // Weighted calculation (volume 50%, float 25%, S/O 25%) - but with lower base scores
  const signalScore = (floatScore * 0.25 + soScore * 0.25 + volumeScore * 0.5) * signalMultiplier * adrMultiplier * soBonus;
  
  return {
    score: parseFloat(Math.min(1.0, signalScore).toFixed(2)),
    floatScore: parseFloat(floatScore.toFixed(2)),
    soScore: parseFloat(soScore.toFixed(2)),
    volumeScore: parseFloat(volumeScore.toFixed(2)),
    signalMultiplier: parseFloat(signalMultiplier.toFixed(2)),
    adrMultiplier: parseFloat(adrMultiplier.toFixed(2)),
    soBonus: parseFloat(soBonus.toFixed(2)),
    isADR: adrMultiplier > 1.0,
    isCustodianVerified: isCustodianVerified,
    custodianName: custodianName
  };
};

// Filing Time Multiplier - 1.2x boost for 30 mins before/after market open & close (9:30am & 4:00pm ET)
// 30 mins before/after open = strongest potential for price moves
const getFilingTimeMultiplier = (filingDateString) => {
  try {
    const filingTime = new Date(filingDateString);
    const etTime = new Date(filingTime.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const hours = etTime.getHours();
    const minutes = etTime.getMinutes();
    const totalMinutes = hours * 60 + minutes;
    
    let timeMultiplier = 1.0;
    
    // Peak: 9:00-10:00am (540-600) & 3:30-4:30pm (930-1020) = 1.2x (30 mins before/after market open/close)
    if ((totalMinutes >= 540 && totalMinutes <= 600) || (totalMinutes >= 930 && totalMinutes <= 1020)) {
      timeMultiplier = 1.2;
    }
    // Strong: 8:30-11:00am (510-660) & 2:30-5:00pm (870-1080) = 1.15x
    else if ((totalMinutes >= 510 && totalMinutes <= 660) || (totalMinutes >= 870 && totalMinutes <= 1080)) {
      timeMultiplier = 1.15;
    }
    // Good: 8:00am-12:00pm (480-720) & 2:00pm-5:30pm (840-1110) = 1.10x
    else if ((totalMinutes >= 480 && totalMinutes <= 720) || (totalMinutes >= 840 && totalMinutes <= 1110)) {
      timeMultiplier = 1.10;
    }
    // Other trading hours: 4am-6pm (240-1080) = 1.05x
    else if (totalMinutes >= 240 && totalMinutes <= 1080) {
      timeMultiplier = 1.05;
    }
    // Outside 4am-6pm: no bonus
    else {
      timeMultiplier = 1.0;
    }
    
    return timeMultiplier;
  } catch (e) {
    return 1.0;
  }
};

// Global Attention Window Bonus - TIER system for max gap-up potential
// 18:01 (Asian open), 13:21 (Euro close/US lunch), 21:01 (Overnight dark pool)
const getGlobalAttentionBonus = (filingDateString) => {
  try {
    const filingTime = new Date(filingDateString);
    const etTime = new Date(filingTime.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const hours = etTime.getHours();
    const minutes = etTime.getMinutes();
    
    let bonus = 1.0;
    let tier = 'None';
    
    // TIER 1: GOLDEN HOURS (1.3x) - Asian open catalyst + overnight dark pool
    // 18:00-19:00 ET (Asia waking up at 7am+)
    // 21:00-22:00 ET (whole night for accumulation, Asia at 10am+ midday)
    if ((hours === 18 && minutes <= 59) || (hours === 21 && minutes <= 59)) {
      bonus = 1.3;
      tier = 'Golden';
      // Closeness bonus: exact :01 = 1.05x multiplier, :59 = 1.01x
      const closenessBonus = 1.0 + (Math.max(0, 60 - Math.abs(minutes - 1)) / 1200);
      bonus = bonus * closenessBonus;
    }
    // TIER 2: SILVER HOURS (1.15x) - Pre/post golden windows
    // 17:00-18:00 ET (pre-Asian open prep)
    // 20:00-21:00 ET (pre-overnight accumulation)
    // 13:00-14:00 ET (post-lunch dead zone, Europe 6pm)
    // 12:00-13:00 ET (lunch start)
    else if ((hours === 17) || (hours === 20) || (hours === 13) || (hours === 12)) {
      bonus = 1.15;
      tier = 'Silver';
      // Closeness bonus for tier 2
      const closenessBonus = 1.0 + (Math.max(0, 60 - Math.abs(minutes - 1)) / 1500);
      bonus = bonus * closenessBonus;
    }
    // TIER 3: BRONZE HOURS (1.05x) - Extended after-hours window
    // 22:00-04:00 ET (dark pool extended hours)
    // 16:00-17:00 ET (afternoon slump before prep)
    // 09:30-12:00 ET (morning session weaker)
    else if ((hours >= 22 || hours <= 4) || (hours === 16) || (hours >= 9 && hours <= 11)) {
      bonus = 1.05;
      tier = 'Bronze';
    }
    
    return { bonus: parseFloat(bonus.toFixed(4)), tier };
  } catch (e) {
    return { bonus: 1.0, tier: 'None' };
  }
};


const log = (level, message) => {
  let titleColor = '\x1b[90m';
  let messageColor = '\x1b[32m';

  if (level === 'ERROR') {
    titleColor = '\x1b[31m';
    messageColor = '\x1b[31m';
  } else if (level === 'WARN') {
    titleColor = '\x1b[33m';
    messageColor = '\x1b[33m';
  } else if (level === 'ALERT') {
    titleColor = '\x1b[91m';
    messageColor = '\x1b[91m';
  } else if (level === 'INFO') {
    titleColor = '\x1b[92m';
    messageColor = '\x1b[92m';
  }

  console.log(`\x1b[90m[${new Date().toISOString()}] ${titleColor}${level}: ${messageColor}${message}\x1b[0m`);
};

const FORM_TYPES = ['6-K', '6-K/A', '8-K', '8-K/A', 'S-1', 'S-3', 'S-4', 'S-8', 'F-1', 'F-3', 'F-4', '424B1', '424B2', '424B3', '424B4', '424B5', '424H8', '20-F', '20-F/A', '13G', '13G/A', '13D', '13D/A', 'Form D', 'EX-99.1', 'EX-99.2', 'EX-10.1', 'EX-10.2', 'EX-3.1', 'EX-3.2', 'EX-4.1', 'EX-4.2', 'EX-10.3', 'EX-1.1', 'Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04', 'Item 1.05', 'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06', 'Item 3.01', 'Item 3.02', 'Item 3.03', 'Item 4.01', 'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08', 'Item 5.09', 'Item 5.10', 'Item 5.11', 'Item 5.12', 'Item 5.13', 'Item 5.14', 'Item 5.15', 'Item 6.01', 'Item 7.01', 'Item 8.01', 'Item 9.01'];
const SEMANTIC_KEYWORDS = {
  'Merger/Acquisition': ['Merger Agreement', 'Acquisition Agreement', 'Agreed To Acquire', 'Merger Consideration', 'Premium Valuation', 'Going Private', 'Take Private'],
  'FDA Granted': ['FDA Approval', 'FDA Clearance', 'EMA Approval', 'Breakthrough Therapy', 'Fast Track Designation', 'Priority Review'],
  'Clinical Success': ['Positive Trial Results', 'Phase 3 Success', 'Topline Results Beat', 'Efficacy Demonstrated', 'Safety Profile Met'],
  'Capital Raise': ['Oversubscribed', 'Institutional Participation', 'Lead Investor', 'Top-Tier Investor', 'Strategic Investor'],
  'Earnings Beat': ['Earnings Beat', 'Beat Expectations', 'Beat Consensus', 'Exceeded Guidance', 'Record Revenue'],
  'Major Contract': ['Contract Award', 'Major Customer Win', 'Strategic Partnership', '$100 Million Contract', 'Exclusive License'],
  'Regulatory Approval': ['Regulatory Approval Granted', 'Patent Approved', 'License Granted', 'Permit Issued'],
  'Revenue Growth': ['Revenue Growth Acceleration', 'Record Quarterly Revenue', 'Guidance Raise', 'Organic Growth'],
  'Insider Buying': ['Director Purchase', 'Executive Purchase', 'CEO Buying', 'CFO Buying', 'Meaningful Accumulation'],
  'Reverse Split': ['Reverse Stock Split', 'Reverse Split', 'Reversed Split', 'Reverse Split Announced', 'Announced Reverse Split', '1-for-', 'Consolidation Of Shares', 'Share Consolidation', 'Combine Shares', 'Combined Shares', 'Restructuring Of Capital', 'Stock Consolidation', 'Share Combination', 'Reverse 1:8', 'Reverse 1:10', 'Reverse 1:20', 'Reverse 1:25', 'Reverse 1:50'],
  'Bankruptcy Filing': ['Bankruptcy Protection', 'Chapter 11 Filing', 'Chapter 7 Filing', 'Insolvency Proceedings', 'Creditor Protection'],
  'Going Concern': ['Going Concern Warning', 'Substantial Doubt Going Concern', 'Auditor Going Concern Note', 'Continued Losses', 'Operating Loss', 'Net Loss', 'Loss from operations', 'Massive Losses', 'Accumulated Deficit', 'Significant loss', 'Substantial losses', 'Cash burn rate', 'Depleted cash', 'Negative cash flow'],
  'Public Offering': ['Public Offering Announced', 'Secondary Offering', 'Follow-On Offering', 'Shelf Offering', 'At-The-Market Offering'],
  'Dilution': ['Dilutive Securities', 'New Shares Issued', 'Convertible Notes', 'Warrant Issuance', 'Option Grants Excessive', 'Shares Outstanding Increased', 'Share Dilution', 'Share Issuance', 'Dilutive issuance', 'Shares increased', 'Share increase', 'Offering shares', 'Issuance of shares', 'Equity incentive plan increase'],
  'Delisting Risk': ['Nasdaq Deficiency', 'Listing Standards Warning', 'Nasdaq Notification', 'Nasdaq Letter', 'Regained Compliance', 'Delisting Risk', 'Minimum Bid Price', 'Delisting Threat'],
  'Warrant Redemption': ['Warrant Redemption Notice', 'Forced Exercise', 'Warrant Call Notice', 'Final Expiration Notice'],
  'Insider Selling': ['Director Sale', 'Officer Sale', 'CEO Selling', 'CFO Selling', 'Massive Liquidation', 'Employee Incentive', 'Equity Compensation', 'RSU Grant', 'Restricted Stock Unit', 'General admin expense', 'Stock-based compensation', 'Excessive compensation', 'Compensation increase', 'Executive compensation spike'],
  'Accounting Restatement': ['Financial Restatement', 'Audit Non-Reliance', 'Material Weakness', 'Control Deficiency', 'Audit Adjustment'],
  'Credit Default': ['Loan Default', 'Debt Covenant Breach', 'Event Of Default', 'Credit Agreement Violation'],
  'Debt Issuance': ['Debt Issuance', 'Senior Debt Issued', 'Convertible Bonds', 'Junk Bond Offering', 'High Leverage'],
  'Material Lawsuit': ['Material Litigation', 'Lawsuit Filed', 'Major Lawsuit', 'SEC Investigation', 'DOJ Investigation'],
  'Supply Chain Crisis': ['Supply Chain Disruption', 'Production Halt', 'Factory Closure', 'Supplier Bankruptcy', 'Shipping Delays'],
  'Executive Departure': ['CEO Departed', 'CFO Departed', 'CEO Resigned', 'Board Resignation', 'Chief Officer Left'],
  'Asset Impairment': ['Goodwill Impairment', 'Asset Write-Down', 'Impairment Charge', 'Valuation Adjustment'],
  'Restructuring': ['Organizational Restructure', 'Cost Reduction Program', 'Efficiency Initiative', 'Division Realignment'],
  'Stock Buyback': ['Share Repurchase', 'Buyback Authorization', 'Accelerated Buyback', 'Repurchase Program'],
  'Licensing Deal': ['Exclusive License', 'License Agreement', 'Technology License', 'IP Licensing'],
  'Partnership': ['Strategic Partnership', 'Joint Venture', 'Partnership Agreement', 'Strategic Alliance'],
  'Facility Expansion': ['New Facility Opening', 'Capacity Expansion', 'Manufacturing Expansion', 'Facility Upgrade'],
  'Blockchain Initiative': ['Blockchain Integration', 'Cryptocurrency Payment', 'NFT Launch', 'Web3 Partnership', 'Token Launch', 'Smart Contract Deployment', 'Blockchain Adoption', 'Crypto Exchange Partnership', 'Decentralized Platform'],
  'Government Contract': ['Government Contract Award', 'Defense Contract', 'Federal Contract', 'DOD Contract', 'GSA Schedule', 'Federal Procurement'],
  'Stock Split': ['Stock Split Announced', 'Forward Split', 'Stock Dividend', 'Share Split'],
  'Dividend Increase': ['Dividend Increase', 'Dividend Hike', 'Special Dividend', 'Increased Dividend', 'Quarterly Dividend Raised', 'Annual Dividend Increase'],
  'Compliance Issue': ['Regulatory Violation', 'FDA Warning', 'Product Recall', 'Safety Recall', 'Warning Letter', 'Compliance Violation', 'Regulatory Enforcement', 'VIE structure', 'VIE agreement', 'PRC regulations', 'regulatory risk', 'capital control', 'foreign exchange restriction', 'dividend limitation', 'SAFE Circular', 'Chinese regulatory', 'Subject to risks', 'Uncertainty of interpretation', 'variable interest'],
  'Mining Operations': ['Mining Operation', 'Cryptocurrency Mining', 'Blockchain Mining', 'Bitcoin Mining', 'Ethereum Mining', 'Mining Facility', 'Mining Expansion', 'Hash Rate Growth'],
  'Financing Events': ['IPO Announced', 'Debt Offering', 'Credit Facility', 'Loan Facility', 'Financing Secured', 'Capital Structure', 'Bond Issuance'],
  'Analyst Coverage': ['Analyst Initiation', 'Analyst Upgrade', 'Analyst Initiation Buy', 'Rating Upgrade', 'Price Target Increase', 'Outperform Rating', 'Buy Rating Initiated'],
  'Product Discontinuation': ['Product Discontinuation', 'Product Discontinue', 'Discontinuing Product', 'Product Line Discontinued', 'End Of Life Product', 'Phase Out Product'],
  'Loss of Major Customer': ['Major Customer Loss', 'Lost Major Customer', 'Customer Concentration Risk', 'Significant Customer Left', 'Key Customer Departure', 'Primary Customer Loss']
};


const SEC_CODE_TO_COUNTRY = {'C2':'Shanghai, China','F4':'Shadong, China','6A':'Shanghai, China','D8':'Hong Kong','H0':'Hong Kong','K3':'Kowloon Bay, Hong Kong','S4':'Singapore','U0':'Singapore','C0':'Cayman Islands','K2':'Cayman Islands','E9':'Cayman Islands','1E':'Charlotte Amalie, U.S. Virgin Islands','VI':'Road Town, British Virgin Islands','A1':'Toronto, Canada','A2':'Winnipeg, Canada','A6':'Ottawa, Canada','A9':'Vancouver, Canada','A0':'Calgary, Canada','CA':'Toronto, Canada','C4':'Toronto, Canada','D0':'Hamilton, Canada','D9':'Toronto, Canada','Q0':'Toronto, Canada','L3':'Tel Aviv, Israel','J1':'Tokyo, Japan','M0':'Tokyo, Japan','E5':'Dublin, Ireland','I0':'Dublin, Ireland','L2':'Dublin, Ireland','DE':'Wilmington, Delaware','1T':'Athens, Greece','B2':'Bridgetown, Barbados','B6':'Nassau, Bahamas','B9':'Hamilton, Bermuda','C1':'Buenos Aires, Argentina','C3':'Brisbane, Australia','C7':'St. Helier, Channel Islands','D2':'Hamilton, Bermuda','D4':'Hamilton, Bermuda','D5':'Sao Paulo, Brazil','D6':'Bridgetown, Barbados','E4':'Hamilton, Bermuda','F2':'Frankfurt, Germany','F3':'Paris, France','F5':'Johannesburg, South Africa','G0':'St. Helier, Jersey','G1':'St. Peter Port, Guernsey','G4':'New York, United States','G7':'Copenhagen, Denmark','H1':'St. Helier, Jersey','I1':'Douglas, Isle of Man','J0':'St. Helier, Jersey','J2':'St. Helier, Jersey','J3':'St. Helier, Jersey','K1':'Seoul, South Korea','K7':'New York, United States','L0':'Hamilton, Bermuda','L6':'Milan, Italy','M1':'Majuro, Marshall Islands','N0':'Amsterdam, Netherlands','N2':'Amsterdam, Netherlands','N4':'Amsterdam, Netherlands','O5':'Mexico City, Mexico','P0':'Lisbon, Portugal','P3':'Manila, Philippines','P7':'Madrid, Spain','P8':'Warsaw, Poland','R0':'Milan, Italy','S0':'Madrid, Spain','T0':'Lisbon, Portugal','T3':'Johannesburg, South Africa','U1':'London, United Kingdom','U5':'London, United Kingdom','V0':'Zurich, Switzerland','V8':'Geneva, Switzerland','W0':'Frankfurt, Germany','X0':'London, UK','X1':'Luxembourg City, Luxembourg','Y0':'Nicosia, Cyprus','Y1':'Nicosia, Cyprus','Z0':'Johannesburg, South Africa','Z1':'Johannesburg, South Africa','1A':'Pago Pago, American Samoa','1B':'Saipan, Northern Mariana Islands','1C':'Hagatna, Guam','1D':'San Juan, Puerto Rico','3A':'Sydney, Australia','4A':'Auckland, New Zealand','5A':'Apia, Samoa','7A':'Moscow, Russia','8A':'Mumbai, India','9A':'Jakarta, Indonesia','2M':'Frankfurt, Germany','U3':'Madrid, Spain','Y9':'Nicosia, Cyprus','AL':'Birmingham, UK','Q8':'Oslo, Norway','R1':'Panama City, Panama','V7':'Stockholm, Sweden','K8':'Jakarta, Indonesia','O9':'Monaco','W8':'Istanbul, Turkey','R5':'Lima, Peru','N8':'Kuala Lumpur, Malaysia'};

const parseSemanticSignals = (text) => {
  if (!text) return {};
  const lowerText = text.toLowerCase();
  const signals = {};
  
  for (const [category, keywords] of Object.entries(SEMANTIC_KEYWORDS)) {
    const matches = keywords.filter(kw => {
      const escaped = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(`\\b${escaped}\\b`, 'i');
      return regex.test(lowerText);
    });
    if (matches.length > 0) {
      signals[category] = matches;
    }
  }
  
  return signals;
};

const extractReverseSplitRatio = (text) => {
  if (!text) return null;
  // Match patterns like "1-for-10", "1-for-20", "1 for 10", etc.
  const ratioMatch = text.match(/1\s*(?:-|for)\s*(\d+)/i);
  if (ratioMatch && ratioMatch[1]) {
    return `1-for-${ratioMatch[1]}`;
  }
  return null;
};

const getExchangePrefix = (ticker) => {
  // Map tickers to their exchanges for TradingView
  // Detect exchange based on ticker format, length, and patterns
  
  if (!ticker || ticker === 'Unknown') return 'NASDAQ';
  
  const upperTicker = ticker.toUpperCase();
  
  // OTC/Pink Sheet Indicators:
  // 1. Ticker length >= 5 characters (ABCDE format)
  // 2. Contains non-alphabetic characters (XXXX.L, XXXX.V, etc.)
  // 3. Ends with common OTC suffixes (.OB, .PK, .OTC, .BB)
  // 4. Explicit OTC mentions
  if (upperTicker.length >= 5) {
    return 'OTC';
  }

  // Non-alphabetic characters indicate international or OTC
  if (/[^A-Z]/.test(upperTicker)) {
    return 'OTC';
  }
  
  // Known NYSE stocks (blue chips) - map specific high-volume tickers
  const nyseStocks = ['F', 'GM', 'BAC', 'C', 'JPM', 'GE', 'XOM', 'CVX', 'T', 'VZ', 'WMT', 'KO', 'PEP', 'MCD', 'IBM', 'PG', 'JNJ', 'KMB', 'MRK', 'PFE'];
  if (nyseStocks.includes(upperTicker)) {
    return 'NYSE';
  }
  
  // Default to NASDAQ for 1-4 letter alphabetic tickers
  return 'NASDAQ';
};

const cleanupStaleAlerts = () => {
  try {
    if (!fs.existsSync(CONFIG.ALERTS_FILE)) return;
    
    const alerts = JSON.parse(fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8'));
    if (!Array.isArray(alerts) || alerts.length === 0) return;
    
    const now = new Date();
    const dayOfWeek = now.getDay(); // 0 = Sunday, 1 = Monday, 5 = Friday, 6 = Saturday
    
    // Monday-Thursday: wipe after 7 days
    // Friday-Sunday: wipe after 5 days (less stale data over weekend)
    const daysToKeep = (dayOfWeek >= 1 && dayOfWeek <= 4) ? 7 : 5;
    const cutoffTime = now.getTime() - (daysToKeep * 24 * 60 * 60 * 1000);
    
    const filtered = alerts.filter(alert => {
      const alertTime = new Date(alert.recordedAt).getTime();
      return alertTime > cutoffTime;
    });
    
    if (filtered.length < alerts.length) {
      const removed = alerts.length - filtered.length;
      fs.writeFileSync(CONFIG.ALERTS_FILE, JSON.stringify(filtered, null, 2));
      log('INFO', `Cleanup: Removed ${removed} stale alerts (${daysToKeep} day policy)`);
    }
  } catch (err) {
    // Cleanup error - don't break the app
  }
};

const saveToCSV = (alertData) => {
  try {
    const csvPath = CONFIG.CSV_FILE;
    const headers = 'Filed Date,Filed Time,Scanned Date,Scanned Time,CIK,Ticker,Price,Score,Float,Shares Outstanding,S/O Ratio,Volume,Average Volume,Incorporated,Located,Filing Type,Catalyst,Custodian Control,Filing Time Bonus,S/O Bonus,Skip Reason\n';
    
    // Create file with headers if it doesn't exist
    if (!fs.existsSync(csvPath)) {
      fs.writeFileSync(csvPath, headers);
    }
    
    // Format filing timestamp
    const filingTime = new Date(alertData.filingDate);
    const filedDate = filingTime.toISOString().split('T')[0];
    const filedTime = filingTime.toTimeString().split(' ')[0];
    
    // Format scan timestamp
    const now = new Date();
    const scannedDate = now.toISOString().split('T')[0];
    const scannedTime = now.toTimeString().split(' ')[0];
    
    // Format signals/intent as readable string
    const signals = (alertData.intent && Array.isArray(alertData.intent)) 
      ? alertData.intent.join('; ').replace(/,/g, ';')
      : (alertData.intent ? String(alertData.intent).replace(/,/g, ';') : 'N/A');
    
    // Extract country (last part after comma if exists)
    let incorporated = alertData.incorporated || 'Unknown';
    if (incorporated.includes(',')) {
      const parts = incorporated.split(',');
      incorporated = parts[parts.length - 1].trim();
    }
    
    let located = alertData.located || 'Unknown';
    if (located.includes(',')) {
      const parts = located.split(',');
      located = parts[parts.length - 1].trim();
    }
    
    // Helper function to safely escape CSV values
    const escapeCSV = (value) => {
      if (value === null || value === undefined) return 'N/A';
      const str = String(value);
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    };
    
    // Build CSV row with data
    const row = [
      escapeCSV(filedDate),
      escapeCSV(filedTime),
      escapeCSV(scannedDate),
      escapeCSV(scannedTime),
      escapeCSV(alertData.cik || 'N/A'),
      escapeCSV(alertData.ticker || 'N/A'),
      escapeCSV(alertData.price || 'N/A'),
      escapeCSV(alertData.signalScore || 'N/A'),
      escapeCSV(alertData.float || 'N/A'),
      escapeCSV(alertData.sharesOutstanding || 'N/A'),
      escapeCSV(alertData.soRatio || 'N/A'),
      escapeCSV(alertData.volume || 'N/A'),
      escapeCSV(alertData.averageVolume || 'N/A'),
      escapeCSV(incorporated || 'N/A'),
      escapeCSV(located || 'N/A'),
      escapeCSV(alertData.filingType || 'N/A'),
      escapeCSV(signals || 'Press/Regulatory Release'),
      escapeCSV(alertData.custodianControl ? (alertData.custodianVerified ? `1.3x ${alertData.custodianName}` : alertData.custodianName) : 'No'),
      escapeCSV(alertData.filingTimeBonus ? `${alertData.filingTimeBonus}x Filing Time` : 'No'),
      escapeCSV(alertData.soBonus && alertData.soBonus > 1.0 ? `${alertData.soBonus}x S/O` : 'No'),
      escapeCSV(alertData.skipReason || ''),
    ];

    // Convert to CSV string
    const csvRow = row.join(',') + '\n';
    fs.appendFileSync(csvPath, csvRow);
  } catch (err) {
    log('WARN', `CSV save failed: ${err.message}`);
  }
};

const saveAlert = (alertData) => {
  try {
    let alerts = [];
    if (fs.existsSync(CONFIG.ALERTS_FILE)) {
      const content = fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8').trim();
      if (content) {
        try {
          alerts = JSON.parse(content);
          if (!Array.isArray(alerts)) alerts = [];
        } catch (e) {
          alerts = [];
        }
      }
    }
    
    const enrichedData = {
      ...alertData,
      recordedAt: new Date().toISOString(),
      recordId: `${alertData.ticker}-${Date.now()}`,
      expiresAt: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000).toISOString()
    };
    
    alerts.push(enrichedData);
    if (alerts.length > 1000) alerts = alerts.slice(-1000);
    
    fs.writeFileSync(CONFIG.ALERTS_FILE, JSON.stringify(alerts, null, 2));
    
    // Determine direction for CSV - check for ANY bearish signals
    const bearishCategories = ['Reverse Split', 'Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Dilution', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis', 'Compliance Issue', 'Product Discontinuation', 'Loss of Major Customer'];
    const signalKeys = (alertData.intent && Array.isArray(alertData.intent)) ? alertData.intent : (alertData.intent ? String(alertData.intent).split(', ') : []);
    const hasBearish = signalKeys.some(cat => bearishCategories.includes(cat));
    const direction = hasBearish ? 'SHORT' : 'LONG';
    const reason = (alertData.intent && Array.isArray(alertData.intent)) 
      ? alertData.intent.join('; ')
      : (alertData.intent ? String(alertData.intent) : 'Filing');
    
    // Update alert data with skip reason showing it was alerted
    const bonusItems = [];
    if (alertData.hasTuesdayBonus) bonusItems.push('Tuesday 1.2x');
    if (alertData.custodianControl) {
      const custodianLabel = alertData.custodianVerified ? `${alertData.custodianName} 1.3x` : `${alertData.custodianName} 1.15x`;
      bonusItems.push(custodianLabel);
    }
    if (alertData.filingTimeBonus) bonusItems.push(`Filing Time ${alertData.filingTimeBonus}x`);
    if (alertData.soBonus && alertData.soBonus > 1.0) bonusItems.push(`S/O ${alertData.soBonus}x`);
    const bonusIndicator = bonusItems.length > 0 ? ` (${bonusItems.join(' + ')} score bonus)` : '';
    alertData.skipReason = `Alert sent: [${direction}] ${reason}${bonusIndicator}`;
    
    // Save to CSV for analysis
    saveToCSV(alertData);
    
    // Cleanup stale alerts based on day of week
    cleanupStaleAlerts();
    
    if (Object.keys(alertData.signals || {}).length > 0) {
      let stocks = [];
      if (fs.existsSync(CONFIG.STOCKS_FILE)) {
        const content = fs.readFileSync(CONFIG.STOCKS_FILE, 'utf8').trim();
        if (content) {
          try {
            stocks = JSON.parse(content);
            if (!Array.isArray(stocks)) stocks = [];
          } catch (e) {
            stocks = [];
          }
        }
      }
      stocks.push(enrichedData);
      if (stocks.length > 5000) stocks = stocks.slice(-5000);
      fs.writeFileSync(CONFIG.STOCKS_FILE, JSON.stringify(stocks, null, 2));
    }
    
    sendPersonalWebhook(alertData);
    
    // Update performance tracking data for HTML dashboard
    updatePerformanceData(alertData);
    
    log('INFO', `Log: Alert saved ${alertData.ticker} (pushed to GitHub)`);
    
    const volDisplay = alertData.volume && alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'n/a';
    const avgVolDisplay = alertData.averageVolume && alertData.averageVolume !== 'N/A' ? (alertData.averageVolume / 1000000).toFixed(2) + 'm' : 'n/a';
    const floatDisplay = alertData.float && alertData.float !== 'N/A' && !isNaN(alertData.float) ? (alertData.float / 1000000).toFixed(2) + 'm' : 'n/a';
    const soDisplay = alertData.soRatio || 'n/a';
    
    const priceDisplay = alertData.price && alertData.price !== 'N/A' ? `$${alertData.price.toFixed(2)}` : 'N/A';
    const formTypeStr = Array.isArray(alertData.formType) ? (alertData.formType[0] || '6-K') : (alertData.formType || '6-K');
    const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${alertData.cik}&type=${formTypeStr}&dateb=&owner=exclude&count=100`;
    const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(alertData.ticker)}:${alertData.ticker}`;
    log('INFO', `Links: ${secLink} ${tvLink}`);
    console.log('');
    
    try {
      if (fs.existsSync(CONFIG.ALERTS_FILE)) {
        const savedAlerts = JSON.parse(fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8'));
        const lastAlert = savedAlerts[savedAlerts.length - 1];
        if (lastAlert && lastAlert.recordId === enrichedData.recordId) {
          // Alert saved successfully
        }
      }
    } catch (verifyErr) {
      // Verification failed silently
    }
  } catch (err) {
    log('ERROR', `Failed to save alert: ${err.message}`);
  }
  
  pushToGitHub();
};

// Update performance tracking data for alerts (for HTML dashboard)
const updatePerformanceData = (alertData) => {
  try {
    let performanceData = {};
    
    // Load existing performance data
    if (fs.existsSync(CONFIG.PERFORMANCE_FILE)) {
      const content = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8').trim();
      if (content) {
        try {
          performanceData = JSON.parse(content);
          if (!performanceData || typeof performanceData !== 'object') {
            performanceData = {};
          }
        } catch (e) {
          performanceData = {};
        }
      }
    }
    
    const ticker = alertData.ticker;
    const currentPrice = parseFloat(alertData.price) || 0;
    
    // Initialize or update ticker performance data
    if (!performanceData[ticker]) {
      performanceData[ticker] = {
        short: alertData.short ? true : false,
        alert: currentPrice,
        highest: currentPrice,
        lowest: currentPrice,
        current: currentPrice,
        currentPrice: currentPrice,  // Live price - gets updated by price fetcher
        performance: 0,  // Will be calculated when live price is available
        date: new Date().toISOString(),
        reverseSplitRatio: null
      };
    } else {
      // Update current price and track peaks/lows
      performanceData[ticker].current = currentPrice;
      if (currentPrice > performanceData[ticker].highest) {
        performanceData[ticker].highest = currentPrice;
      }
      if (currentPrice < performanceData[ticker].lowest) {
        performanceData[ticker].lowest = currentPrice;
      }
    }
    
    // Calculate performance metrics
    const alertPrice = performanceData[ticker].alert || 0;
    if (alertPrice > 0) {
      const change = currentPrice - alertPrice;
      const percentChange = (change / alertPrice) * 100;
      performanceData[ticker].performance = parseFloat(percentChange.toFixed(2));
      performanceData[ticker].reverseSplitRatio = null; // Can be updated if needed
    }
    
    // Write updated performance data
    fs.writeFileSync(CONFIG.PERFORMANCE_FILE, JSON.stringify(performanceData, null, 2));
    
  } catch (err) {
    log('WARN', `Failed to update performance data: ${err.message}`);
  }
};

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper: Fetch with proper timeout using AbortController
const fetchWithTimeout = async (url, timeoutMs = 5000, options = {}) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(timeout);
  }
};

// Fetch float data from Financial Modeling Prep
// Get shares outstanding from Alpha Vantage (primary)
const getSharesFromAlphaVantage = async (ticker) => {
  try {
    const avKey = process.env.ALPHA_VANTAGE_API_KEY;
    if (!avKey) return null;
    
    const res = await fetchWithTimeout(`https://www.alphavantage.co/query?function=OVERVIEW&symbol=${ticker}&apikey=${avKey}`, 5000);
    if (!res.ok) return null;
    
    const data = await res.json();
    if (data.SharesOutstanding && data.SharesOutstanding !== 'None') {
      return Math.round(parseInt(data.SharesOutstanding) || 0) || null;
    }
    return null;
  } catch (e) {
    return null;
  }
};

// Get shares outstanding from Finnhub (secondary)
const getSharesFromFinnhub = async (ticker) => {
  try {
    const finnhubKey = process.env.FINNHUB_API_KEY;
    if (!finnhubKey) return null;
    
    const res = await fetchWithTimeout(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`, 5000);
    if (!res.ok) return null;
    
    const data = await res.json();
    if (data.shareOutstanding && data.shareOutstanding > 0) {
      return Math.round(data.shareOutstanding);
    }
    return null;
  } catch (e) {
    return null;
  }
};

// Get shares outstanding with priority: Alpha Vantage → Finnhub → FMP
const getSharesOutstanding = async (ticker) => {
  // Try Finnhub first (most reliable, already called for profile)
  try {
    const finnhubKey = process.env.FINNHUB_API_KEY;
    if (finnhubKey) {
      const res = await fetchWithTimeout(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`, 3000);
      if (res.ok) {
        const data = await res.json();
        if (data.shareOutstanding && data.shareOutstanding > 0) {
          return Math.round(data.shareOutstanding);
        }
      }
    }
  } catch (e) {}
  
  return 'N/A';
};

// Get float data from Alpha Vantage first, then FMP as fallback
const getFloatData = async (ticker) => {
  // Try Alpha Vantage first (has both SharesFloat and SharesOutstanding)
  try {
    const avKey = process.env.ALPHA_VANTAGE_API_KEY;
    if (avKey) {
      const res = await fetchWithTimeout(`https://www.alphavantage.co/query?function=OVERVIEW&symbol=${ticker}&apikey=${avKey}`, 3000);
      if (res.ok) {
        const data = await res.json();
        if (data.SharesFloat && data.SharesFloat !== 'None') {
          const float = Math.round(parseInt(data.SharesFloat) || 0);
          if (float > 0) return float;
        }
      }
    }
  } catch (e) {}
  
  // Fallback to FMP
  try {
    const fmpKey = process.env.FMP_API_KEY;
    if (!fmpKey) return 'N/A';
    
    const url = `https://financialmodelingprep.com/stable/shares-float?symbol=${ticker}&apikey=${fmpKey}`;
    const res = await fetchWithTimeout(url, 3000);
    if (!res.ok) return 'N/A';
    
    const data = await res.json();
    if (Array.isArray(data) && data[0] && data[0].floatShares) {
      return Math.round(data[0].floatShares);
    }
    return 'N/A';
  } catch (e) {
    return 'N/A';
  }
};

// Fetch quote data - only used as fallback when Yahoo/Finnhub fail
const getFMPQuote = async (ticker) => {
  try {
    const fmpKey = process.env.FMP_API_KEY;
    if (!fmpKey) return null;
    
    const res = await fetchWithTimeout(`https://financialmodelingprep.com/stable/shares-float?symbol=${ticker}&apikey=${fmpKey}`, 5000);
    if (!res.ok) return null;
    
    const data = await res.json();
    if (!Array.isArray(data) || !data[0]) return null;
    
    const d = data[0];
    
    return {
      regularMarketPrice: 'N/A', // FMP shares-float doesn't have price
      regularMarketVolume: 0,
      marketCap: 'N/A',
      sharesOutstanding: d.outstandingShares ? Math.round(d.outstandingShares) : 'N/A',
      averageDailyVolume3Month: 0,
      floatShares: d.floatShares ? Math.round(d.floatShares) : 'N/A'
    };
  } catch (e) {
    return null;
  }
};

async function fetchFilings() {
  const allFilings = [];
  
  try {
    await wait(200);
    const res = await fetchWithTimeout('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=6-K&count=100&owner=exclude&output=atom', CONFIG.SEC_FETCH_TIMEOUT || 10000, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
      }
    });
    if (res.ok) {
      const xml = await res.text();
      const entries = xml.split('<entry>').slice(1);
      for (const entry of entries) {
        const title = entry.match(/<title[^>]*>(.*?)<\/title>/s)?.[1];
        const link = entry.match(/<link[^>]*href="([^"]+)"/)?.[1];
        const updated = entry.match(/<updated[^>]*>(.*?)<\/updated>/)?.[1];
        if (!title || !link || !updated) continue;
        const ageMin = (Date.now() - new Date(updated).getTime()) / (1000 * 60);
        if (ageMin > CONFIG.FILE_TIME) continue; // Only recent filings (1 minute)
        const cik = link.match(/\/data\/(\d+)\//)?.[1];
        allFilings.push({ txtLink: link, title, cik, updated, source: 'SEC', formType: '6-K' });
      }
    }
    await rateLimit.wait();
  } catch (err) {
    // Silently skip abort errors (expected timeouts), only log other errors
    if (err.name !== 'AbortError' && !err.message.includes('aborted')) {
      log('WARN', `SEC 6-K fetch failed: ${err.message}`);
    }
  }

  allFilings.sort((a, b) => new Date(b.updated) - new Date(a.updated));
  return allFilings.slice(0, 100);
}

const isValidTicker = ticker => {
  if (!ticker || ticker.length < 1 || ticker.length > 5) return false;
  return /^[A-Z]+$/.test(ticker);
};

async function getCountryAndTicker(cik) {
  const stateCountryFallback = {
    'DE': 'Delaware', 'CA': 'California', 'NY': 'New York', 'TX': 'Texas', 'FL': 'Florida',
    'WA': 'Washington', 'IL': 'Illinois', 'PA': 'Pennsylvania', 'OH': 'Ohio', 'GA': 'Georgia',
    'MI': 'Michigan', 'NC': 'North Carolina', 'NJ': 'New Jersey', 'VA': 'Virginia', 'MA': 'Massachusetts',
    'AZ': 'Arizona', 'TN': 'Tennessee', 'IN': 'Indiana', 'MD': 'Maryland', 'CO': 'Colorado',
    'MN': 'Minnesota', 'MO': 'Missouri', 'WI': 'Wisconsin', 'UT': 'Utah', 'NV': 'Nevada',
    'NM': 'New Mexico', 'CT': 'Connecticut', 'OK': 'Oklahoma', 'IA': 'Iowa', 'OR': 'Oregon',
    'KS': 'Kansas', 'AR': 'Arkansas', 'MS': 'Mississippi', 'LA': 'Louisiana', 'KY': 'Kentucky',
    'SC': 'South Carolina', 'AL': 'Alabama', 'WV': 'West Virginia', 'NE': 'Nebraska', 'ID': 'Idaho',
    'HI': 'Hawaii', 'AK': 'Alaska', 'VT': 'Vermont', 'ME': 'Maine', 'MT': 'Montana',
    'RI': 'Rhode Island', 'NH': 'New Hampshire', 'WY': 'Wyoming', 'ND': 'North Dakota', 'SD': 'South Dakota',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico', 'VI': 'U.S. Virgin Islands', 'GU': 'Guam',
    'AS': 'American Samoa', 'MP': 'Northern Mariana Islands',
    'CN': 'China', 'HK': 'Hong Kong', 'SG': 'Singapore', 'IL': 'Israel', 'JP': 'Japan',
    'IE': 'Ireland', 'KY': 'Cayman Islands', 'VG': 'British Virgin Islands', 'CA': 'Canada',
    'GB': 'United Kingdom', 'CH': 'Switzerland', 'DE': 'Germany', 'FR': 'France', 'BR': 'Brazil',
    'A8': 'Montreal, Canada'
  };
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const padded = cik.toString().padStart(10, '0');
      await wait(500);
      const res = await fetchWithTimeout(`https://data.sec.gov/submissions/CIK${padded}.json`, CONFIG.SEC_FETCH_TIMEOUT, {
        headers: {
          'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
          'Accept': 'application/json'
        }
      });
      if (res.status === 403) {
        await wait(2000);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();

      let incorporated = '';
      let located = '';

      if (data.stateOfIncorporation) {
        incorporated = data.stateOfIncorporation;
      } else if (data.incorporated && data.incorporated.stateOrCountry) {
        incorporated = data.incorporated.stateOrCountry;
      } else if (data.incorporated && data.incorporated.country) {
        incorporated = data.incorporated.country;
      }

      if (data.addresses && data.addresses.business && data.addresses.business.stateOrCountry) {
        located = data.addresses.business.stateOrCountry;
      } else if (data.addresses && data.addresses.business && data.addresses.business.country) {
        located = data.addresses.business.country;
      } else if (data.addresses && data.addresses.mailing && data.addresses.mailing.stateOrCountry) {
        located = data.addresses.mailing.stateOrCountry;
      } else if (data.addresses && data.addresses.mailing && data.addresses.mailing.country) {
        located = data.addresses.mailing.country;
      }

      if (!incorporated && data.entityType && /^[A-Z]{2}$/.test(data.entityType)) {
        incorporated = data.entityType;
      }

      let incorporatedDisplay = 'Unknown';
      let locatedDisplay = 'Unknown';
      
      if (incorporated) {
        incorporatedDisplay = SEC_CODE_TO_COUNTRY[incorporated] || stateCountryFallback[incorporated] || incorporated;
      }
      if (located) {
        locatedDisplay = SEC_CODE_TO_COUNTRY[located] || stateCountryFallback[located] || located;
      }

      return {
        incorporated: incorporatedDisplay,
        located: locatedDisplay,
        ticker: data.tickers?.[0] || 'Unknown'
      };
    } catch (err) {
      log('WARN', `SEC lookup attempt ${attempt} failed for CIK ${cik}: ${err.message}`);
      if (attempt < 3) await wait(5000);
    }
  }
  return { incorporated: 'Unknown', located: 'Unknown', ticker: 'Unknown' };
}

async function fetch8Ks() {
  const filings8K = [];
  try {
    await wait(200);
    const res = await fetchWithTimeout('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=100&owner=exclude&output=atom', 15000, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      }
    });
    if (res.ok) {
      const xml = await res.text();
      const entries = xml.split('<entry>').slice(1);
      for (const entry of entries) {
        const title = entry.match(/<title[^>]*>(.*?)<\/title>/s)?.[1];
        const link = entry.match(/<link[^>]*href="([^"]+)"/)?.[1];
        const updated = entry.match(/<updated[^>]*>(.*?)<\/updated>/)?.[1];
        if (!title || !link || !updated) continue;
        const ageMin = (Date.now() - new Date(updated).getTime()) / (1000 * 60);
        if (ageMin > CONFIG.FILE_TIME) continue;
        const cik = link.match(/\/data\/(\d+)\//)?.[1];
        filings8K.push({ txtLink: link, title, cik, updated, source: 'SEC', formType: '8-K' });
      }
    }
    await rateLimit.wait();
  } catch (err) {
    // Silently skip abort errors (expected timeouts), only log other errors
    if (err.name !== 'AbortError' && !err.message.includes('aborted')) {
      log('WARN', `SEC 8-K fetch failed: ${err.message}`);
    }
  }
  return filings8K;
}

async function getFilingText(indexUrl) {
  for (let attempt = 1; attempt <= CONFIG.MAX_RETRY_ATTEMPTS; attempt++) {
    try {
      await wait(100); // Minimal delay, rate limiter handles the rest
      const res = await Promise.race([
        fetch(indexUrl, {
          headers: {
            'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
          }
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('SEC index fetch timeout')), CONFIG.SEC_FETCH_TIMEOUT))
      ]);

      if (res.status === 403) {
        log('WARN', `SEC blocked request (403), waiting 5 seconds`);
        await wait(5000);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      
      const html = await Promise.race([
        res.text(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('SEC index text parse timeout')), CONFIG.SEC_FETCH_TIMEOUT))
      ]);
      
      const docHrefs = [];
      
      // Simple non-backtracking regex to extract links
      const hrefMatches = html.match(/href="([^"]+)"/g) || [];
      for (const hrefMatch of hrefMatches) {
        const href = hrefMatch.replace(/^href="/, '').replace(/"$/, '');
        const lower = href.toLowerCase();
        if ((lower.endsWith('.txt') || lower.endsWith('.html') || lower.endsWith('.htm')) && !lower.includes('index')) {
          docHrefs.push(href);
        }
      }
      
      // If no documents found, try a more permissive search
      if (docHrefs.length === 0) {
        const allLinks = html.match(/href="([^"]+\.(?:txt|html|htm))"/gi) || [];
        for (const link of allLinks) {
          const href = link.replace(/^href="/, '').replace(/"$/i, '');
          const lower = href.toLowerCase();
          if (!lower.includes('index') && !lower.includes('style')) {
            docHrefs.push(href);
          }
        }
      }
      
      const txtFiles = docHrefs.filter(href => href.toLowerCase().endsWith('.txt'));
      const htmlFiles = docHrefs.filter(href => !href.toLowerCase().endsWith('.txt'));
      
      const prioritizedHrefs = txtFiles.length > 0 ? txtFiles : htmlFiles;
      if (prioritizedHrefs.length === 0) throw new Error(`No filing documents found at ${indexUrl}`);
      
      let combinedText = '';
      const MAX_COMBINED_SIZE = CONFIG.MAX_COMBINED_SIZE;
      
      for (const href of prioritizedHrefs.slice(0, 2)) {
        const fullUrl = href.startsWith('http') ? href : `https://www.sec.gov${href}`;        
        
        try {
          const docRes = await Promise.race([
            fetch(fullUrl, {
              headers: {
                'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
              }
            }),
            new Promise((_, reject) => setTimeout(() => reject(new Error('SEC document fetch timeout')), CONFIG.SEC_FETCH_TIMEOUT))
          ]);

          let docText = await Promise.race([
            docRes.text(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('SEC document text parse timeout')), CONFIG.SEC_FETCH_TIMEOUT))
          ]);
          
          const lowerText = docText.toLowerCase();
          if ((lowerText.includes('sec.gov') && lowerText.includes('search filings')) ||
              lowerText.includes('sec home') || 
              lowerText.includes('filing detail') ||
              lowerText.includes('edgar latest filings') ||
              (lowerText.includes('<table') && lowerText.includes('column heading') && lowerText.length < 5000)) {
            continue; // Skip navigation/index pages
          }
          
          if (docText.length > 500000) {
            docText = docText.slice(0, 500000);
          }
          
          let cleanText = docText
            .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
            .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
            .replace(/<!--[\s\S]*?-->/g, '')
            .replace(/&nbsp;/g, ' ')
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&#(\d+);/g, (match, code) => String.fromCharCode(parseInt(code)))
            .replace(/<[^>]+>/g, ' ')
            .replace(/\d{10}-\d{2}-\d{6}\.\w+\s*:\s*\d+\s*\d{10}-\d{2}-\d{6}\.\w+/g, '')
            .replace(/^\s*(?:exhibit|annex|appendix|schedule|form|section)\s+[a-z0-9]+\s*\n/gim, '')
            .replace(/(?:table of contents|index to exhibits|signatures|certification|forward-looking statements|risk factors)/gi, '')
            .replace(/(?:page \d+|continued|see page|see exhibit|see schedule)/gi, '')
            .replace(/filed\s+(?:on\s+)?[\d\-\/]*/gi, '')
            .replace(/(?:sec\.?gov|edgar|securities and exchange|s\.e\.c\.|rule \d+-\d+)/gi, '')
            .replace(/\s+/g, ' ')
            .trim();
          
          combinedText += cleanText + ' ';
          
          docText = null;
          cleanText = null;
          
          await rateLimit.wait();
          
          if (combinedText.length > MAX_COMBINED_SIZE) break;
        } catch (docErr) {
          log('DEBUG', `Document fetch error for ${fullUrl}: ${docErr.message}`);
          continue;
        }
      }
      
      combinedText = combinedText
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, MAX_COMBINED_SIZE);
      
      return combinedText;
    } catch (err) {
      log('ERROR', `Filing text fetch attempt ${attempt}/3 failed: ${err.message}`);
      if (attempt < 3) await wait(3000);
    }
  }
  log('ERROR', `Failed to fetch filing text after 3 attempts from ${indexUrl}`);
  return '';
}

const sendPersonalWebhook = (alertData) => {
  try {
    // Skip if no webhook URL configured
    if (!CONFIG.PERSONAL_WEBHOOK_URL) {
      return;
    }
    
    const { ticker, price, intent, incorporated, located } = alertData;
    
    const combinedLocation = (incorporated || '').toLowerCase() + ' ' + (located || '').toLowerCase();
    
    const allowed = CONFIG.ALLOWED_COUNTRIES.some(country => combinedLocation.includes(country));
    if (!allowed) {
      return;
    }
    
    const countryCodeMap = {
      'israel': 'IL', 'china': 'CN', 'hong kong': 'HK', 'cayman': 'KY', 'japan': 'JP', 'california': 'US',
      'virgin islands': 'VG', 'singapore': 'SG', 'canada': 'CA', 'ireland': 'IE', 'delaware': 'US',
      'alabama': 'US', 'alaska': 'US', 'arizona': 'US', 'arkansas': 'US', 'colorado': 'US',
      'connecticut': 'US', 'delaware': 'US', 'florida': 'US', 'georgia': 'US', 'hawaii': 'US',
      'idaho': 'US', 'illinois': 'US', 'indiana': 'US', 'iowa': 'US', 'kansas': 'US',
      'kentucky': 'US', 'louisiana': 'US', 'maine': 'US', 'maryland': 'US', 'massachusetts': 'US',
      'michigan': 'US', 'minnesota': 'US', 'mississippi': 'US', 'missouri': 'US', 'montana': 'US',
      'nebraska': 'US', 'nevada': 'US', 'new hampshire': 'US', 'new jersey': 'US', 'new mexico': 'US',
      'new york': 'US', 'north carolina': 'US', 'north dakota': 'US', 'ohio': 'US', 'oklahoma': 'US',
      'oregon': 'US', 'pennsylvania': 'US', 'rhode island': 'US', 'south carolina': 'US', 'south dakota': 'US',
      'tennessee': 'US', 'texas': 'US', 'utah': 'US', 'vermont': 'US', 'virginia': 'US',
      'washington': 'US', 'west virginia': 'US', 'wisconsin': 'US', 'wyoming': 'US', 'district of columbia': 'US'
    };
    
    const countryLower = (located || incorporated || 'Unknown').toLowerCase();
    let countryCode = 'XX';
    for (const [country, code] of Object.entries(countryCodeMap)) {
      if (countryLower.includes(country)) {
        countryCode = code;
        break;
      }
    }
    
    const incLower = (incorporated || '').toLowerCase();
    const locLower = (located || '').toLowerCase();
    let incorporatedCode = 'XX';
    let locatedCode = 'XX';
    
    for (const [country, code] of Object.entries(countryCodeMap)) {
      if (incLower.includes(country) && incorporatedCode === 'XX') {
        incorporatedCode = code;
      }
      if (locLower.includes(country) && locatedCode === 'XX') {
        locatedCode = code;
      }
    }
    
    const countryDisplay = incorporatedCode === locatedCode ? incorporatedCode : `${incorporatedCode}/${locatedCode}`;
    
    // Determine direction based on bearish signal categories
    const bearishCategories = ['Reverse Split', 'Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Dilution', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis', 'Compliance Issue', 'Product Discontinuation', 'Loss of Major Customer'];
    const intentArray = (intent && Array.isArray(intent)) ? intent : (intent ? String(intent).split(', ') : []);
    const hasBearish = intentArray.some(cat => bearishCategories.includes(cat));
    const direction = hasBearish ? 'SHORT' : 'LONG';
    const reason = (intent && Array.isArray(intent)) ? intent.join(', ').substring(0, 50).toLowerCase() : (intent || 'Filing').toString().substring(0, 50).toLowerCase();
    const priceDisplay = price && price !== 'N/A' ? `$${parseFloat(price).toFixed(2)}` : 'N/A';
    const volDisplay = alertData.volume && alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'N/A';
    const avgDisplay = alertData.averageVolume && alertData.averageVolume !== 'N/A' ? (alertData.averageVolume / 1000000).toFixed(2) + 'm' : 'n/a';
    
    // Calculate volume multiplier
    let volumeMultiplier = '';
    if (alertData.volume && alertData.averageVolume && alertData.volume !== 'N/A' && alertData.averageVolume !== 'N/A') {
      const ratio = alertData.volume / alertData.averageVolume;
      if (ratio >= 2) {
        volumeMultiplier = ` (${ratio.toFixed(1)}x)`;
      }
    }
    
    const floatDisplay = alertData.float && alertData.float !== 'N/A' ? (alertData.float / 1000000).toFixed(2) + 'm' : 'N/A';
    const signalScoreBold = alertData.signalScore ? `**${alertData.signalScore}**` : 'N/A';
    const signalScoreDisplay = alertData.signalScore ? alertData.signalScore : 'N/A';
    
    const personalAlertContent = `↳ [${direction}] **$${ticker}** @ ${priceDisplay} (${countryDisplay}), score: ${signalScoreBold}, ${reason}, vol/avg: ${volDisplay}/${avgDisplay}${volumeMultiplier}, float: ${floatDisplay}, s/o: ${alertData.soRatio}
    https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
    const personalMsg = { content: personalAlertContent };
    
    log('INFO', `Alert: [${direction}] $${ticker} @ ${priceDisplay}, Score: ${signalScoreDisplay}, Float: ${alertData.float !== 'N/A' ? (alertData.float / 1000000).toFixed(2) + 'm' : 'N/A'}, Vol/Avg: ${volDisplay}/${avgDisplay}, S/O: ${alertData.soRatio}`);
    
    // Non-blocking fetch with timeout
    Promise.race([
      fetch(CONFIG.PERSONAL_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(personalMsg),
        timeout: 5000
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Webhook timeout')), 6000))
    ]).catch(err => {
      // Silently fail - don't block on webhook
    });
  } catch (err) {
    // Silently fail - don't block processing
  }
};

const pushToGitHub = () => {
  try {
    const projectRoot = CONFIG.GITHUB_REPO_PATH;
    // Run git push in background, don't wait for it
    require('child_process').exec(`cd ${projectRoot} && git add logs/alert.json logs/stocks.json 2>/dev/null && git commit -m "Auto: Alert update $(date '+%Y-%m-%d %H:%M:%S')" 2>/dev/null && git push origin main 2>/dev/null`, { 
      timeout: 5000 // 5 second timeout for git operations
    }, (error) => {
      if (error && !error.message.includes('timeout')) {
        // Silently fail if not timeout
      }
    });
  } catch (err) {
    // Git operations failed silently
  }
};

const app = express();

app.get('/logs/alert.json', (req, res) => {
  try {
    if (fs.existsSync(CONFIG.ALERTS_FILE)) {
      const data = fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8');
      res.setHeader('Content-Type', 'application/json');
      res.send(data);
    } else {
      res.json([]);
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/logs/stocks.json', (req, res) => {
  try {
    if (fs.existsSync(CONFIG.STOCKS_FILE)) {
      const data = fs.readFileSync(CONFIG.STOCKS_FILE, 'utf8');
      res.setHeader('Content-Type', 'application/json');
      res.send(data);
    } else {
      res.json([]);
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/logs/quote.json', (req, res) => {
  try {
    if (fs.existsSync(CONFIG.PERFORMANCE_FILE)) {
      const data = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8');
      res.setHeader('Content-Type', 'application/json');
      res.send(data);
    } else {
      res.json({});
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/git-status', (req, res) => {
  try {
    const projectRoot = '/home/user/Documents/sysd';
    const status = execSync(`cd ${projectRoot} && git status --porcelain 2>/dev/null`, { encoding: 'utf8' }).trim();
    const lastCommit = execSync(`cd ${projectRoot} && git log -1 --pretty=format:"%h - %s (%ai)" 2>/dev/null`, { encoding: 'utf8' }).trim();
    const branch = execSync(`cd ${projectRoot} && git rev-parse --abbrev-ref HEAD 2>/dev/null`, { encoding: 'utf8' }).trim();
    
    log('INFO', `Git: Last commit: ${lastCommit || 'No commits'}`);
    
    res.json({
      status: 'online',
      branch: branch || 'main',
      lastCommit: lastCommit || 'No commits',
      workingTree: status || 'Clean',
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ 
      status: 'error', 
      message: err.message,
      timestamp: new Date().toISOString()
    });
  }
});

app.get('/', (req, res) => {
  res.sendFile('./docs/index.html', { root: '.' });
});

app.use(express.static('./docs'));

// Quote endpoint with Yahoo → FMP → Finnhub fallback
app.get('/api/quote/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  
  try {
    // Try Yahoo Finance first
    let quote = await yahooFinance.quote(ticker, {
      fields: ['regularMarketPrice', 'regularMarketVolume', 'marketCap', 'exchange'],
    }).catch(() => null);
    
    // If Yahoo fails, try FMP
    if (!quote || !quote.regularMarketPrice) {
      const finnhubKey = process.env.FINNHUB_API_KEY;
      if (finnhubKey) {
        try {
          const finnhubRes = await fetchWithTimeout(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${finnhubKey}`, 5000);
          if (finnhubRes.ok) {
            const data = await finnhubRes.json();
            // Finnhub data structure: c=current, v=volume
            if (data.c && data.c > 0) {
              quote = {
                symbol: ticker,
                regularMarketPrice: data.c,
                regularMarketVolume: data.v || 0,
                marketCap: 'N/A',
                sharesOutstanding: 'N/A',
                averageDailyVolume3Month: 0,
                exchange: 'UNKNOWN'
              };
              
              // Get profile for shares and market cap
              try {
                const profRes = await fetchWithTimeout(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`, 5000);
                if (profRes.ok) {
                  const prof = await profRes.json();
                  if (prof.shareOutstanding && prof.shareOutstanding > 0) {
                    quote.sharesOutstanding = Math.round(prof.shareOutstanding);
                  }
                  if (prof.marketCapitalization && prof.marketCapitalization > 0) {
                    quote.marketCap = Math.round(prof.marketCapitalization * 1000000);
                  }
                }
              } catch (e) {}
            }
          }
        } catch (e) {
          // Silently fail Finnhub fallback
        }
      }
    }
    
    // If Finnhub failed, try FMP for shares outstanding and float
    if (!quote || !quote.regularMarketPrice) {
      quote = await getFMPQuote(ticker);
    }
    
    // Try to get fundamental data from alert.json for this ticker
    let fundamentals = {};
    try {
      if (fs.existsSync(CONFIG.ALERTS_FILE)) {
        const alerts = JSON.parse(fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8'));
        const latestAlert = alerts.filter(a => a.ticker === ticker).pop();
        if (latestAlert) {
          fundamentals = {
            float: latestAlert.float || 'N/A',
            sharesOutstanding: latestAlert.sharesOutstanding || 'N/A',
            soRatio: latestAlert.soRatio || 'N/A',
            averageVolume: latestAlert.averageVolume || 0
          };
        }
      }
    } catch (e) {
      // Silently fail if alert.json doesn't exist
    }
    
    // If no float data in alerts, try FMP as fallback
    if (!fundamentals.float || fundamentals.float === 'N/A') {
      fundamentals.float = quote?.floatShares || await getFloatData(ticker);
    }
    
    // If no shares outstanding in alerts, try: Alpha Vantage → Finnhub → FMP
    if (!fundamentals.sharesOutstanding || fundamentals.sharesOutstanding === 'N/A') {
      fundamentals.sharesOutstanding = quote?.sharesOutstanding || await getSharesOutstanding(ticker);
    }
    
    res.json({
      symbol: ticker,
      price: quote?.regularMarketPrice || 'N/A',
      volume: quote?.regularMarketVolume || 0,
      averageVolume: fundamentals.averageVolume || quote?.averageDailyVolume3Month || 0,
      marketCap: quote?.marketCap || 'N/A',
      exchange: quote?.exchange || 'UNKNOWN',
      float: fundamentals.float || 'N/A',
      sharesOutstanding: fundamentals.sharesOutstanding || 'N/A',
      soRatio: fundamentals.soRatio || 'N/A',
    });
    
    // Update performance data if price is available
    if (quote?.regularMarketPrice && quote.regularMarketPrice > 0) {
      try {
        let performanceData = {};
        if (fs.existsSync(CONFIG.PERFORMANCE_FILE)) {
          const content = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8').trim();
          if (content) {
            try {
              performanceData = JSON.parse(content);
              if (!performanceData || typeof performanceData !== 'object') {
                performanceData = {};
              }
            } catch (e) {
              performanceData = {};
            }
          }
        }
        
        if (performanceData[ticker]) {
          const currentPrice = quote.regularMarketPrice;
          performanceData[ticker].currentPrice = currentPrice;
          if (currentPrice > performanceData[ticker].highest) {
            performanceData[ticker].highest = currentPrice;
          }
          if (currentPrice < performanceData[ticker].lowest) {
            performanceData[ticker].lowest = currentPrice;
          }
          
          // Recalculate performance
          const alertPrice = performanceData[ticker].alert;
          if (alertPrice > 0) {
            const change = currentPrice - alertPrice;
            const percentChange = (change / alertPrice) * 100;
            performanceData[ticker].performance = parseFloat(percentChange.toFixed(2));
          }
          
          fs.writeFileSync(CONFIG.PERFORMANCE_FILE, JSON.stringify(performanceData, null, 2));
        }
      } catch (e) {
        // Silently fail performance update
      }
    }
  } catch (error) {
    log('ERROR', `Quote endpoint error for ${ticker}: ${error.message}`);
    res.json({
      symbol: ticker,
      price: 'N/A',
      volume: 0,
      averageVolume: 0,
      marketCap: 'N/A',
      exchange: 'UNKNOWN',
      float: 'N/A',
      sharesOutstanding: 'N/A',
      soRatio: 'N/A'
    });
  }
});

app.post('/api/clear-alerts', (req, res) => {
  try {
    const alertsFile = CONFIG.ALERTS_FILE;
    // Write empty array to alerts file
    fs.writeFileSync(alertsFile, '[]');
    res.json({ success: true, message: 'Alerts cleared' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/ping', (req, res) => {
  try {
    res.json({ status: 'online', onlineUsers: 1 });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// Background task to update performance data for all tracked stocks
const updateAllPerformanceData = async () => {
  try {
    if (!fs.existsSync(CONFIG.PERFORMANCE_FILE)) return;
    
    const perfContent = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8').trim();
    if (!perfContent) return;
    
    let performanceData = {};
    try {
      performanceData = JSON.parse(perfContent);
      if (!performanceData || typeof performanceData !== 'object') return;
    } catch (e) {
      return;
    }
    
    let updated = false;
    
    // Update each tracked stock with delay to avoid rate limits
    for (const ticker of Object.keys(performanceData)) {
      try {
        const quote = await yahooFinance.quote(ticker, {
          fields: ['regularMarketPrice'],
        }).catch(() => null);
        
        if (quote && quote.regularMarketPrice && quote.regularMarketPrice > 0) {
          const currentPrice = quote.regularMarketPrice;
          performanceData[ticker].currentPrice = currentPrice;
          
          // Track highest/lowest
          if (currentPrice > (performanceData[ticker].highest || 0)) {
            performanceData[ticker].highest = currentPrice;
          }
          if (currentPrice < (performanceData[ticker].lowest || currentPrice)) {
            performanceData[ticker].lowest = currentPrice;
          }
          
          // Recalculate performance
          const alertPrice = performanceData[ticker].alert;
          if (alertPrice > 0) {
            const change = currentPrice - alertPrice;
            const percentChange = (change / alertPrice) * 100;
            performanceData[ticker].performance = parseFloat(percentChange.toFixed(2));
          }
          
          updated = true;
        }
      } catch (e) {
        // Silently skip individual ticker errors
      }
      
      // Add small delay between requests to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    
    // Write back if any updates were made
    if (updated) {
      fs.writeFileSync(CONFIG.PERFORMANCE_FILE, JSON.stringify(performanceData, null, 2));
    }
  } catch (e) {
    // Silently fail background update
  }
};

// Run performance update every 30 seconds
setInterval(updateAllPerformanceData, 30000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  log('INFO', `App: Dashboard online at https://eugenesnonprofit.com/ & http://localhost:${PORT}`);
});

(async () => {
  let cycleCount = 0, alertsSent = 0, startTime = Date.now();
  let processedHashes = new Map(); // Pure in-memory, session-based (100 max)
  let loggedFetch = false;
  
  try {
    const projectRoot = '/home/user/Documents/sysd';
    const branch = execSync(`cd ${projectRoot} && git rev-parse --abbrev-ref HEAD`, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }).trim() || 'main';
    const lastCommit = execSync(`cd ${projectRoot} && git log -1 --pretty=format:"%h - %s (%ai)"`, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }).trim() || 'No commits';
    log('INFO', `Git: Last commit: ${lastCommit}`);
  } catch (err) {
  }
    
 while (true) {
    try {
      cycleCount++;
      const filings6K = await Promise.race([
        fetchFilings(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('fetch timeout')), 30000))
      ]).catch(() => []);
      const filings8K = await Promise.race([
        fetch8Ks(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('fetch timeout')), 30000))
      ]).catch(() => []);
      const allFilings = [...filings6K, ...filings8K];
      
      // Log only if there are new filings and we haven't logged this batch yet
      let newFilingFound = false;
      for (const filing of allFilings) {
        const hash = crypto.createHash('md5').update(filing.title + filing.updated).digest('hex');
        if (!processedHashes.has(hash)) {
          newFilingFound = true;
          break;
        }
      }
      
      if (newFilingFound) {
        const form6KCount = allFilings.filter(f => f.formType === '6-K').length;
        const form8KCount = allFilings.filter(f => f.formType === '8-K').length;
        log('INFO', `Fetched ${allFilings.length} filings: 6-K: ${form6KCount} / 8-K: ${form8KCount}`);
        console.log('');
      }
      
      const filingsToProcess = allFilings.slice(0, 100);
      for (let i = 0; i < filingsToProcess.length; i++) {
        const filing = filingsToProcess[i];
        let skipReason = ''; // Track why alert is skipped
        try {
          const hash = crypto.createHash('md5').update(filing.title + filing.updated).digest('hex');
          
          if (processedHashes.has(hash)) {
            continue;
          }
          
          processedHashes.set(hash, Date.now());
          
          const filingTime = new Date(filing.updated);
          const filingDate = filingTime.toLocaleString('en-US', { timeZone: 'America/New_York' });
          const text = await Promise.race([
            getFilingText(filing.txtLink),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Filing text fetch timeout')), CONFIG.SEC_FETCH_TIMEOUT * 3))
          ]).catch(() => '');
          
          if (!text) {
            log('WARN', `Failed to fetch filing text for ${filing.txtLink}`);
            console.log('');
        
            continue;
          }
          
          let semanticSignals = parseSemanticSignals(text);
          
          let source = 'SEC';
          let intent = Object.keys(semanticSignals).join(', ') || null;
          
          // Intent fallback - skip file headers and extract first real sentence
          if (!intent && text) {
            // Remove common file headers and boilerplate
            let cleanText = text
              .replace(/^[^a-z]*?\d{10}-\d{2}-\d{6}[^.]*\./im, '')
              .replace(/^[^a-z]*?EXHIBITS[^.]*\./im, '')
              .replace(/^[^a-z]*?(?:EXHIBIT|INDEX|INFORMATION CONTAINED)[^.]*\./im, '')
              .replace(/^[^a-z]*?(?:form\s*6-?k|period of report|filed|certification)[^.]*\./im, '')
              .replace(/^[^a-z]*?\d{10}-\d{2}-\d{6}\.\w+\s*:\s*\d+\s*\d{10}-\d{2}-\d{6}\.\w+[^.]*\./im, '') // Remove SEC metadata like "0001292814-25-004426.txt : 20251230 0001292814-25-004426.hdr"
              .replace(/^[^a-z]*?SEC\.GOV[^.]*\./im, '')
              .replace(/^[^a-z]*?EDGAR[^.]*\./im, '')
              .replace(/^[^a-z]*?(?:table of contents|company information|item\s+\d+|exhibit|schedule|appendix|annex)[^.]*\./im, '')
              .replace(/^[^a-z]*?(?:signatures|certification|forward-looking|risk factors)[^.]*\./im, '')
              .trim();
            
            // Get first sentence that's longer than 20 chars
            const sentences = cleanText.match(/[^.!?]*[.!?]/);
            if (sentences && sentences[0]) {
              let firstSentence = sentences[0]
                .replace(/^\s+|\s+$/g, '')
                .replace(/\d+\s*of\s*\d+/g, '')
                // Remove common boilerplate words
                .replace(/\b(?:exhibit|item|form|section|schedule|annex|appendix|certification|pursuant|hereby|thereof|thereto|incorporated|organized|registrant|issuer|sec\.?gov|edgar|rule\s+\d+)/gi, '')
                // Remove filing metadata
                .replace(/\b(?:page|pages|continued|see|table|contents|index|filed|effective|period|fiscal|calendar|quarterly|annual)\b/gi, '')
                .replace(/[^\w\s]/g, ' ')
                .replace(/\s+/g, ' ')
                .trim();
              
              if (firstSentence.length > 20 && firstSentence.length < 200) {
                intent = firstSentence;
              }
            }
          }
          
          console.log(`\x1b[90m[${new Date().toISOString()}] % (${filing.title.slice(0,60)}...) - Filed @ ${filingDate} ET\x1b[0m`);
          
          const periodOfReport = filing.updated.split('T')[0];
          
          let ticker = 'Unknown';
          let normalizedIncorporated = 'Unknown';
          let normalizedLocated = 'Unknown';
          
          if (filing.cik) {
            const secData = await getCountryAndTicker(filing.cik);
            ticker = secData.ticker || 'Unknown';
            normalizedIncorporated = secData.incorporated || 'Unknown';
            normalizedLocated = secData.located || 'Unknown';
          }
          
          if (normalizedIncorporated !== normalizedLocated) {
            log('INFO', `Incorporated: ${normalizedIncorporated}, Located: ${normalizedLocated}`);
          } else {
            log('INFO', `Incorporated: ${normalizedIncorporated}`);
          }

          
          if (Object.keys(semanticSignals).length > 0) {
            const allKeywords = [];
            for (const [category, keywords] of Object.entries(semanticSignals)) {
              allKeywords.push(...keywords);
            }
            let newsDisplay = allKeywords.join(', ');
            
            // If "Reverse Split" is detected, try to extract the ratio
            if (Object.keys(semanticSignals).includes('Reverse Split')) {
              const ratio = extractReverseSplitRatio(text);
              if (ratio) {
                // Replace in both the display and the actual signals array
                newsDisplay = newsDisplay.replace(/1-for-/i, ratio + ' ');
                // Also update the semanticSignals array to replace incomplete '1-for-' with complete ratio
                semanticSignals['Reverse Split'] = semanticSignals['Reverse Split'].map(kw => 
                  kw === '1-for-' ? ratio : kw
                );
              }
            }
            
            log('INFO', `News: ${newsDisplay}`);
          } else if (intent) {
            log('INFO', `News: Regulatory Update`);
          } else {
            log('INFO', `News: Press Release`);
          }
          
          const foundForms = new Set();
          const foundItems = new Set();
          const titleAndText = (filing.title + ' ' + text).toLowerCase();
          
          for (const form of FORM_TYPES) {
            if (titleAndText.includes(form.toLowerCase())) {
              foundForms.add(form);
            }
          }
          
          const itemMatches = text.match(/\bItem\s+([1-9]\.\d{2})/gi);
          if (itemMatches) {
            itemMatches.forEach(match => {
              const itemCode = match.match(/[1-9]\.\d{2}/)[0];
              foundItems.add(itemCode);
            });
          }
          
          const mainForms = ['6-K', '6-K/A', '8-K', '8-K/A', 'S-1', 'S-3', 'S-4', 'S-8', 'F-1', 'F-3', 'F-4', '424B1', '424B2', '424B3', '424B4', '424B5', '424H8', '20-F', '20-F/A', '13G', '13G/A', '13D', '13D/A', 'Form D', 'EX-99.1', 'EX-99.2', 'EX-99.3', 'EX-10.1', 'EX-10.2', 'EX-10.3', 'EX-3.1', 'EX-3.2', 'EX-4.1', 'EX-4.2', 'EX-1.1'];
          const mainItems = ['1.01', '1.02', '1.03', '1.04', '1.05', '1.06', '2.01', '2.02', '2.03', '2.04', '2.05', '2.06', '3.01', '3.02', '3.03', '4.01', '4.02', '5.01', '5.02', '5.03', '5.04', '5.05', '5.06', '5.07', '5.08', '5.09', '5.10', '5.11', '5.12', '5.13', '5.14', '5.15', '6.01', '6.02', '7.01', '8.01', '9.01', '9.02', '10.01', '10.02', '10.03', '10.04'];
          const otherForms = Array.from(foundForms).filter(f => mainForms.includes(f));
          const otherItems = Array.from(foundItems).filter(i => mainItems.includes(i));
          const formsDisplay = otherForms.length > 0 ? otherForms.join(', ') : '';
          const itemsDisplay = otherItems.length > 0 ? otherItems.sort((a, b) => parseFloat(a) - parseFloat(b)).map(item => `Item ${item}`).join(', ') : '';
          
          const bearishCategories = ['Reverse Split', 'Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Dilution', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis', 'Compliance Issue', 'Product Discontinuation', 'Loss of Major Customer'];
          const signalKeys = Object.keys(semanticSignals);
          const hasBearish = signalKeys.some(cat => bearishCategories.includes(cat));
          const direction = hasBearish ? 'Short' : 'Long';
          
          let formLogMessage = '';
          if (formsDisplay && formsDisplay !== '') {
            formLogMessage = formsDisplay;
          }
          if (itemsDisplay && itemsDisplay !== '') {
            if (formLogMessage) formLogMessage += ', ' + itemsDisplay;
            else formLogMessage = itemsDisplay;
          }
          if (!formLogMessage) formLogMessage = 'None';
          log('INFO', `Forms: ${formLogMessage}`);
          
          if (signalKeys.length > 0) {
            log('INFO', `${direction}: ${signalKeys.join(', ')}`);
          }
          
          let price = 'N/A', volume = 0, marketCap = 'N/A', averageVolume = 0, float = 'N/A', sharesOutstanding = 'N/A';
          
          if (ticker !== 'UNKNOWN' && isValidTicker(ticker)) {
            try {
              let quoteData = null;
              const finnhubKey = process.env.FINNHUB_API_KEY;
              
              // Try Yahoo FIRST as primary source
              try {
                quoteData = await Promise.race([
                  yahooFinance.quote(ticker, {
                    fields: ['regularMarketPrice', 'regularMarketVolume', 'marketCap', 'sharesOutstanding', 'averageDailyVolume3Month']
                  }),
                  new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                ]).catch(() => null);
              } catch (e) {}
              
              // If Yahoo didn't work, try Finnhub
              if (!quoteData && finnhubKey) {
                try {
                  const fhRes = await fetchWithTimeout(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${finnhubKey}`, 5000);
                  if (fhRes.ok) {
                    const fhQuote = await fhRes.json();
                    if (fhQuote.c && fhQuote.c > 0) {
                      quoteData = {
                        regularMarketPrice: fhQuote.c,
                        regularMarketVolume: fhQuote.v || 0,
                        marketCap: 'N/A',
                        sharesOutstanding: 'N/A',
                        averageDailyVolume3Month: 0
                      };
                      
                      // Get profile for shares and market cap
                      try {
                        const profRes = await fetchWithTimeout(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`, 5000);
                        if (profRes.ok) {
                          const prof = await profRes.json();
                          if (prof.shareOutstanding && prof.shareOutstanding > 0) {
                            quoteData.sharesOutstanding = Math.round(prof.shareOutstanding);
                          }
                          if (prof.marketCapitalization && prof.marketCapitalization > 0) {
                            quoteData.marketCap = Math.round(prof.marketCapitalization * 1000000);
                          }
                        }
                      } catch (e) {}
                    }
                  }
                } catch (e) {}
              }
              
              // If still no data, try FMP (has shares outstanding and float)
              if (!quoteData) {
                const fmpData = await getFMPQuote(ticker);
                if (fmpData) {
                  quoteData = fmpData;
                }
              }
              
              if (quoteData) {
                price = quoteData.regularMarketPrice || 'N/A';
                volume = quoteData.regularMarketVolume || 0;
                marketCap = quoteData.marketCap || 'N/A';
                sharesOutstanding = quoteData.sharesOutstanding || 'N/A';
                averageVolume = quoteData.averageDailyVolume3Month || 0;
                
                // Use float from quoteData if available, else fetch separately
                float = quoteData.floatShares || await getFloatData(ticker);
              
              }
              // If sharesOutstanding still missing, try: Alpha Vantage → Finnhub → FMP
              if (sharesOutstanding === 'N/A' || !sharesOutstanding) {
                sharesOutstanding = await getSharesOutstanding(ticker);
              }
            } catch (err) {}
          }
          
          const priceDisplay = price !== 'N/A' ? `$${price.toFixed(2)}` : 'N/A';
          const volDisplay = volume && volume > 0 ? volume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const avgDisplay = averageVolume && averageVolume > 0 ? averageVolume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const mcDisplay = marketCap !== 'N/A' && marketCap > 0 ? '$' + Math.round(marketCap).toLocaleString('en-US') : 'N/A';
          const floatDisplay = float !== 'N/A' ? float.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          let soRatio = 'N/A';
          if (sharesOutstanding !== 'N/A' && float !== 'N/A' && sharesOutstanding > 0 && !isNaN(float) && !isNaN(sharesOutstanding)) {
            const ratio = (float / sharesOutstanding) * 100;
            soRatio = ratio < 100 ? ratio.toFixed(2) + '%' : ratio.toFixed(1) + '%';
          }
          
          let shortOpportunity = null;
          let longOpportunity = null;
          
          // Determine if this is a SHORT or LONG opportunity based on signals
          const sigKeys = Object.keys(semanticSignals || {});
          
          // Reverse Split + Dilution = ALWAYS SHORT
          const hasReverseSplit = sigKeys.includes('Reverse Split');
          const hasDilution = sigKeys.includes('Dilution');
          const isShortCombo = hasReverseSplit && hasDilution;
          
          if (isShortCombo) {
            shortOpportunity = true;
          } else {
            // Bearish signals = SHORT
            const bearishCats = ['Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis', 'Compliance Issue', 'Product Discontinuation', 'Loss of Major Customer'];
            const hasBearishSignal = sigKeys.some(cat => bearishCats.includes(cat));
            
            if (hasBearishSignal) {
              shortOpportunity = true;
            }
          }
          
          const now = new Date();
          const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
          const etHour = etTime.getHours();
          const etMin = etTime.getMinutes();
          const etTotalMin = etHour * 60 + etMin;
          const startMin = 3.5 * 60; // 3:30am = 210 minutes
          const endMin = 18 * 60; // 6:00pm = 1080 minutes
          
          // Calculate signal score early for logging
          const numFloat = (() => { const v = typeof float === 'number' ? float : (typeof float === 'string' && float !== 'N/A' ? parseInt(float) : NaN); return isNaN(v) ? 0 : v; })();
          const numVolume = (() => { const v = typeof volume === 'number' ? volume : (typeof volume === 'string' && volume !== 'N/A' ? parseInt(volume) : NaN); return isNaN(v) ? 0 : v; })();
          const numAvgVol = (() => { const v = typeof averageVolume === 'number' ? averageVolume : (typeof averageVolume === 'string' && averageVolume !== 'N/A' ? parseInt(averageVolume) : NaN); return isNaN(v) ? 1 : v; })();
          const numShares = (() => { const v = typeof sharesOutstanding === 'number' ? sharesOutstanding : (typeof sharesOutstanding === 'string' && sharesOutstanding !== 'N/A' ? parseInt(sharesOutstanding) : NaN); return isNaN(v) ? 1 : v; })();
          
          // Get signal categories early for scoring function
          const signalCategories = Object.keys(semanticSignals || {});
          
          const signalScoreData = calculatesignalScore(numFloat, numShares, numVolume, numAvgVol, signalCategories, normalizedIncorporated, normalizedLocated, text, filing.title);
          
          // Apply Tuesday bonus (1.2x multiplier for better market conditions)
          const dayOfWeek = new Date().getDay(); // 0=Sunday, 2=Tuesday
          const hasTuesdayBonus = dayOfWeek === 2;
          if (hasTuesdayBonus) {
            signalScoreData.score = parseFloat((signalScoreData.score * 1.2).toFixed(2));
          }
          
          // Filing time bonus: 1.2x peak (30 mins before/after open/close), 1.05-1.15x otherwise
          const filingTimeMultiplier = getFilingTimeMultiplier(filing.updated);
          signalScoreData.filingTimeMultiplier = filingTimeMultiplier;
          signalScoreData.score = parseFloat((signalScoreData.score * filingTimeMultiplier).toFixed(2));
          
          // Global attention window bonus (ADDITIONAL - stacks on top)
          const globalAttentionData = getGlobalAttentionBonus(filing.updated);
          signalScoreData.globalAttentionBonus = globalAttentionData.bonus;
          signalScoreData.globalAttentionTier = globalAttentionData.tier;
          signalScoreData.score = parseFloat((signalScoreData.score * globalAttentionData.bonus).toFixed(2));
          
          signalScoreData.score = parseFloat(Math.min(1.0, signalScoreData.score).toFixed(2));
          
          const signalScoreDisplay = signalScoreData.score;
          
          if (shortOpportunity || longOpportunity) {
            log('INFO', `Stock: $${ticker}, Score: ${signalScoreDisplay}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}, ${shortOpportunity || longOpportunity}`);
          } else {
            log('INFO', `Stock: $${ticker}, Score: ${signalScoreDisplay}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}`);
          }
          
          // Check for FDA Approvals and Chinese/Cayman reverse splits that bypass time window filter
          const hasFDAApproval = signalCategories.includes('FDA Granted');
          const isChinaOrCaymanReverseSplit = (normalizedIncorporated === 'China' || normalizedLocated === 'China' || normalizedIncorporated === 'Cayman Islands' || normalizedLocated === 'Cayman Islands') && signalCategories.includes('Reverse Split');
          const highScoreOverride = signalScoreData.score > 0.6; // Score above threshold can bypass time window IF it passes all other filters
          
          if (etTotalMin < startMin || etTotalMin > endMin) {
            // Allow exceptions for: FDA Approvals and Chinese/Cayman reverse splits
            // High score override will be checked later after all other filters pass
            if (!hasFDAApproval && !isChinaOrCaymanReverseSplit) {
              skipReason = `filing received at ${etHour.toString().padStart(2, '0')}:${etMin.toString().padStart(2, '0')} ET (outside 3:30am-6:00pm window)`;
              const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
              const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
              log('INFO', `Links: ${secLink} ${tvLink}`);
              console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
              console.log('');
              // Save to CSV with skip reason
              try {
                const csvData = {
                  ticker,
                  price,
                  signalScore: signalScoreData.score,
                  short: shortOpportunity ? true : false,
                  marketCap: marketCap,
                  float: float,
                  sharesOutstanding: sharesOutstanding,
                  soRatio: soRatio,
                  volume: volume,
                  averageVolume: averageVolume,
                  incorporated: normalizedIncorporated,
                  located: normalizedLocated,
                  intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                  signalScoreData: signalScoreData,
                  filingDate: filing.updated,
                  filingType: formLogMessage,
                  cik: filing.cik,
                  skipReason: skipReason,
                };
                saveToCSV(csvData);
              } catch (csvErr) {
                log('ERROR', `CSV error: ${csvErr.message}`);
              }
              continue;
            }
          }
          if (normalizedLocated === 'Unknown') {
            skipReason = 'no valid country';
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            continue;
          }
          
          // Check if country is whitelisted (check both incorporated and located)
          const incorporatedMatch = CONFIG.ALLOWED_COUNTRIES.some(country => normalizedIncorporated.toLowerCase().includes(country));
          const locatedMatch = CONFIG.ALLOWED_COUNTRIES.some(country => normalizedLocated.toLowerCase().includes(country));
          const countryWhitelisted = incorporatedMatch || locatedMatch;
          if (!countryWhitelisted) {
            skipReason = `Country not whitelisted (${normalizedIncorporated}, ${normalizedLocated})`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            continue;
          }
          
          const floatValue = float !== 'N/A' ? parseFloat(float) : null;
          if (floatValue !== null && floatValue > CONFIG.MAX_FLOAT) {
            skipReason = `Float ${floatValue.toLocaleString('en-US')} exceeds ${(CONFIG.MAX_FLOAT / 1000000).toFixed(0)}m limit`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            continue;
          }
          
          const volumeValue = volume !== 'N/A' ? parseFloat(volume) : null;

          // Determine volume threshold based on signal type (will be calculated later after semantic analysis)
          // Store volume value for later threshold check after signal detection
          const volumeCheckLater = volumeValue;
          
                    let soRatioValue = null;
          if (sharesOutstanding !== 'N/A' && float !== 'N/A' && sharesOutstanding > 0) {
            const so = parseFloat(sharesOutstanding);
            const fl = parseFloat(float);
            if (!isNaN(fl) && !isNaN(so)) {
              soRatioValue = (fl / so) * 100;
            }
          }
          
          const neutralCategories = ['Executive Departure', 'Asset Impairment', 'Restructuring', 'Stock Buyback', 'Licensing Deal', 'Partnership', 'Facility Expansion', 'Blockchain Initiative', 'Government Contract', 'Stock Split', 'Dividend Increase', 'Mining Operations', 'Financing Events', 'Analyst Coverage'];
          const neutralSignals = signalCategories.filter(cat => neutralCategories.includes(cat));
          const nonNeutralSignals = signalCategories.filter(cat => !neutralCategories.includes(cat));
          // signalCategories, hasFDAApproval, isChinaOrCaymanReverseSplit already defined above before time window check

          // S/O ratio now scoring-only (multiplier), not a hard filter. Removed hard filter block.
          
          // Dynamic volume threshold: 20k for biotech signals, 50k for others
          const isBiotechSignal = hasFDAApproval || signalCategories.includes('Clinical Success');
          const minVolumeThreshold = isBiotechSignal ? 20000 : CONFIG.MIN_ALERT_VOLUME;
          
          // Check volume after knowing signal type
          if (volumeCheckLater !== null && volumeCheckLater < minVolumeThreshold) {
            skipReason = `Volume ${volumeCheckLater.toLocaleString('en-US')} below ${(minVolumeThreshold / 1000).toFixed(0)}k minimum (biotech: ${isBiotechSignal ? 'yes' : 'no'})`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            continue;
          }
          
          let validSignals = false;
          
          // Death spiral categories always trigger alerts regardless of score
          const deathSpiralCategories = ['Going Concern', 'Accounting Restatement', 'Bankruptcy Filing', 'Dilution', 'Reverse Split', 'Compliance Issue'];
          const hasDeathSpiral = nonNeutralSignals.some(cat => deathSpiralCategories.includes(cat));
          
          if (isChinaOrCaymanReverseSplit) {
            validSignals = true; // China/Cayman Islands reverse splits always trigger
          } else if (hasFDAApproval) {
            validSignals = true; // FDA Approval is strong enough alone
          } else if (hasDeathSpiral) {
            validSignals = true; // Death spirals always trigger
          } else if (highScoreOverride && signalCategories.length === 1) {
            // High score (>0.6) with single signal overrides time window IF it passes all other filters
            validSignals = true;
          } else if (signalScoreData.score > 0.6 && signalCategories.length === 1) {
            validSignals = true; // Threshold to 0.6 with single signal
          } else if (signalScoreData.volumeScore >= 0.85 && signalCategories.length >= 1) {
            validSignals = true; // Strong volume spike (2x+ average) with any signal
          } else if (neutralSignals.length > 0 && signalCategories.length >= 2) {
            validSignals = true; // Has neutral signal + at least 1 other signal
          } else if (nonNeutralSignals.length >= 2) {
            validSignals = true; // Has 2+ non-neutral signals from different categories
          }
          
          if (!validSignals) {
            skipReason = 'Not enough signal weight';
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            continue;
          }

          // Check if custodian control (ADR structure) applies - verified via filing text or structure
          const custodianControl = signalScoreData.isCustodianVerified || (normalizedIncorporated && normalizedLocated && normalizedIncorporated.toLowerCase() !== normalizedLocated.toLowerCase());
          
          // Filing time bonus: stronger when filed near open/close (9:30am & 3:30pm ET)
          const filingTimeBonus = filingTimeMultiplier > 1.0 ? parseFloat(filingTimeMultiplier.toFixed(2)) : null;
          
          const alertData = {
            ticker: ticker || filing.cik || 'Unknown',
            title: filing.title ? filing.title.replace(/\s*\(\d{10}\)\s*$/, '').trim() : 'Unknown Company',
            price: price,
            signalScore: signalScoreData.score,
            hasTuesdayBonus: hasTuesdayBonus,
            custodianControl: custodianControl,
            custodianVerified: signalScoreData.isCustodianVerified,
            custodianName: signalScoreData.custodianName,
            filingTimeMultiplier: filingTimeMultiplier,
            filingTimeBonus: filingTimeBonus,
            soBonus: signalScoreData.soBonus,
            volume: volume,
            averageVolume: averageVolume,
            float: float,
            sharesOutstanding: sharesOutstanding,
            soRatio: soRatio,
            marketCap: marketCap,
            short: shortOpportunity ? true : false,
            isShort: shortOpportunity ? true : false,
            intent: intent || 'Regulatory Filing',
            incorporated: normalizedIncorporated,
            located: normalizedLocated,
            filingDate: periodOfReport,
            signals: semanticSignals,
            formType: Array.from(foundForms),
            filingType: formLogMessage,
            cik: filing.cik,
            skipReason: skipReason
          };
          
          // Calculate signal score (already calculated above)
          // signalScore already set at top of alertData
          
          // Only save alert if we got price, float, and S/O data
          if (price !== 'N/A' && float !== 'N/A' && soRatio !== 'N/A') {
            // Check for duplicate alert - don't re-alert the same stock within session
            let isDuplicate = false;
            try {
              if (fs.existsSync(CONFIG.ALERTS_FILE)) {
                const existingAlerts = JSON.parse(fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8'));
                if (Array.isArray(existingAlerts) && existingAlerts.length > 0) {
                  // Check if this ticker was already alerted (last alert)
                  const lastAlert = existingAlerts[existingAlerts.length - 1];
                  if (lastAlert.ticker === ticker) {
                    isDuplicate = true;
                  }
                }
              }
            } catch (e) {
              // If can't read alerts file, proceed without duplicate check
            }
            
            if (isDuplicate) {
              alertData.skipReason = 'Duplicate Alert';
              const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
              const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
              log('INFO', `Links: ${secLink} ${tvLink}`);
              console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, duplicate alert - already alerted in current session\x1b[0m`);
              console.log('');
              // Don't save duplicate alerts
            } else {
              // Set skip reason if this alert has borderline characteristics
              if (signalScoreData.score < 0.3) {
                alertData.skipReason = 'Low Score';
              } else if (Object.keys(semanticSignals).length < 2) {
                alertData.skipReason = 'Not Enough Signals';
              } else if (float !== 'N/A' && parseFloat(float) > CONFIG.MAX_FLOAT * 0.8) {
                alertData.skipReason = 'High Float';
              } else if (volume !== 'N/A' && parseFloat(volume) < 100000) {
                alertData.skipReason = 'Low Volume';
              }
              
              // Only save to alerts if NO skip reason (real alert)
              if (!alertData.skipReason) {
                // NEW FILTER: Only alert on Structure Only + (S/O Bonus OR Filing Time Bonus)
                const hasStructureOnly = signalScoreData.adrMultiplier > 1.0; // Structure Only gives ADR multiplier
                const hasSoBonus = signalScoreData.soBonus > 1.0;
                const hasFilingBonus = filingTimeMultiplier > 1.0 || globalAttentionData.bonus > 1.0;
                
                const meetsWinningPattern = hasStructureOnly && (hasSoBonus || hasFilingBonus);
                
                if (meetsWinningPattern) {
                  saveAlert(alertData);
                } else {
                  // Doesn't meet winning pattern - save to CSV only for tracking
                  alertData.skipReason = 'Pattern Filter (no Structure Only + Bonuses)';
                  saveToCSV(alertData);
                }
              } else {
                // Save borderline stocks to CSV only
                saveToCSV(alertData);
                // Log the skip reason for borderline stocks
                const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
                const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
                log('INFO', `Links: ${secLink} ${tvLink}`);
                console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${alertData.skipReason}\x1b[0m`);
                console.log('');
              }
            }
          } else {
            // Not enough data to process
            if (price === 'N/A') {
              skipReason = 'No Price Data';
            } else if (float === 'N/A') {
              skipReason = 'No Float Data';
            } else if (soRatio === 'N/A') {
              skipReason = 'No S/O Data';
            } else {
              skipReason = 'Incomplete Data';
            }
            
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('INFO', `Quote: Incomplete data for ${ticker} (price: ${price}, float: ${float}, s/o: ${soRatio})`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, ${skipReason}\x1b[0m`);
            console.log('');
            // Save to CSV with skip reason
            try {
              const csvData = {
                ticker,
                price,
                signalScore: signalScoreData.score,
                short: shortOpportunity ? true : false,
                marketCap: marketCap,
                float: float,
                sharesOutstanding: sharesOutstanding,
                soRatio: soRatio,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals) : [],
                signalScoreData: signalScoreData,
                filingDate: filing.updated,
                filingType: formLogMessage,
                cik: filing.cik,
                skipReason: skipReason,
              };
              saveToCSV(csvData);
            } catch (csvErr) {
              log('ERROR', `CSV error: ${csvErr.message}`);
            }
            // Don't save alert if we don't have complete data
          }
        } catch (err) {
          log('WARN', `Filing processing error: ${err.message}`);
        }
      }
      
      if (processedHashes.size > 100) {
        const arr = Array.from(processedHashes.entries())
          .sort((a, b) => b[1] - a[1]) // Sort by time desc
          .slice(0, 80);
        
        processedHashes.clear();
        arr.forEach(([hash, time]) => processedHashes.set(hash, time));
      }
      
      const now = new Date();
      const etTime = now.toLocaleString('en-US', { timeZone: 'America/New_York', hour12: false });
      const etHour = parseInt(etTime.split(', ')[1].split(':')[0]);
      const isPeak = etHour >= 7 && etHour <= 10;
      const isWeekend = now.getDay() === 0 || now.getDay() === 6;
      const isTradingHours = etHour >= 3 && etHour < 18 && !isWeekend;
      
      let refreshInterval;
      if (isWeekend) {
        refreshInterval = CONFIG.REFRESH_WEEKEND;  // 15m on weekends
      } else if (!isTradingHours) {
        refreshInterval = CONFIG.REFRESH_NIGHT;    // 10m outside trading hours
      } else if (isPeak) {
        refreshInterval = CONFIG.REFRESH_PEAK;     // 30s during peak (7am-10am)
      } else {
        refreshInterval = CONFIG.REFRESH_NORMAL;   // 2m during trading hours
      }
      
      await wait(refreshInterval);
    } catch (err) {
      log('ERROR', `Filing loop error: ${err.message}`);
      await wait(60000);
    }
  }

})();