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
const yahooFinance = require('yahoo-finance2').default;

process.env.DEBUG = '';
yahooFinance.suppressNotices(['ripHistorical', 'yahooSurvey']);
yahooFinance.setGlobalConfig({ 
  validation: { logErrors: false, logWarnings: false }
});

const CONFIG = {
  GITHUB_REPO_PATH: process.env.GITHUB_REPO_PATH || '/home/user/Documents/sysd',
  GITHUB_USERNAME: process.env.GITHUB_USERNAME || 'your-github-username',
  GITHUB_REPO_NAME: process.env.GITHUB_REPO_NAME || 'your-repo-name',
  GITHUB_DOMAIN: process.env.GITHUB_DOMAIN || 'your-domain.com',
  PERSONAL_WEBHOOK_URL: process.env.DISCORD_WEBHOOK || '',
  FILE_TIME: 1, // Minutes refresh interval for filings
  MIN_ALERT_VOLUME: 50000, // Min volume threshold
  MAX_FLOAT: 50000000, // Max float size
  MAX_SO_RATIO: 40.0,  // Max short interest ratio
  ALERTS_FILE: 'logs/alert.json', // File to store recent alerts
  STOCKS_FILE: 'logs/stocks.json', // File to store all alerts
  PERFORMANCE_FILE: 'logs/quote.json', // File to store performance data
  ALLOWED_COUNTRIES: ['israel', 'japan', 'china', 'hong kong', 'cayman islands', 'virgin islands', 'singapore', 'canada', 'ireland', 'california', 'delaware'], // Allowed incorporation/located countries
  PI_MODE: true,             // Optimized settings for resource-constrained systems
  REFRESH_PEAK: 1000,        // Check for filings every 1s (peak hours: 7am-10am ET)
  REFRESH_NORMAL: 5000,      // Check for filings every 5s (trading hours: 10am-6pm ET)
  REFRESH_NIGHT: 10000,      // Check for filings every 10s (outside trading hours)
  REFRESH_WEEKEND: 30000,    // Check for filings every 30s (weekends)
  YAHOO_TIMEOUT: 10000,      // Wait max 10s for stock quote data from Yahoo Finance
  SEC_RATE_LIMIT: 3000,      // Wait 3s between SEC filing fetches (API rate limits)
  SEC_FETCH_TIMEOUT: 5000,   // Wait max 5s to fetch filing text from SEC
  MAX_COMBINED_SIZE: 100000, // Max filing size (100KB) before truncating
  MAX_RETRY_ATTEMPTS: 3      // Retry failed API calls up to 3 times
};

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

// Signal Score Calculator
const calculatesignalScore = (float, insiderPercent, institutionalPercent, sharesOutstanding, volume, avgVolume) => {
  // Float Score (smaller is better)
  let floatScore = 0.5;
  const floatMillion = float / 1000000;
  if (floatMillion < 2) floatScore = 1.0;
  else if (floatMillion < 5) floatScore = 0.9;
  else if (floatMillion < 15) floatScore = 0.8;
  else if (floatMillion < 25) floatScore = 0.7;
  else if (floatMillion < 50) floatScore = 0.6;
  
  // Ownership Score (lower % is better) - use insider, fallback to institutional
  let ownershipScore = 0.5;
  const insiderPct = parseFloat(insiderPercent) || 0;
  const instPct = parseFloat(institutionalPercent) || 0;
  
  // Determine if we should use insider or institutional ownership
  let usingInsider = true;
  let effectiveOwnershipPct = insiderPct;
  
  // Use insider if it's valid (> 0 and not NaN)
  if (isNaN(insiderPct) || insiderPct <= 0) {
    // Fall back to institutional ownership if insider is missing/invalid
    usingInsider = false;
    // For institutional ownership, cap at 40% (unusual to see higher)
    effectiveOwnershipPct = Math.min(instPct, 40);
  }
  
  // Score based on which metric we're using
  if (usingInsider) {
    // Insider ownership scoring (stricter - lower is better)
    if (effectiveOwnershipPct < 0.5) ownershipScore = 1.0;
    else if (effectiveOwnershipPct < 2) ownershipScore = 0.9;
    else if (effectiveOwnershipPct < 5) ownershipScore = 0.8;
    else if (effectiveOwnershipPct < 10) ownershipScore = 0.7;
    else if (effectiveOwnershipPct < 20) ownershipScore = 0.6;
  } else {
    // Institutional ownership scoring (more lenient - can be higher)
    // Institutional ownership under 40% is good for pump potential
    if (effectiveOwnershipPct < 10) ownershipScore = 1.0;
    else if (effectiveOwnershipPct < 15) ownershipScore = 0.9;
    else if (effectiveOwnershipPct < 20) ownershipScore = 0.8;
    else if (effectiveOwnershipPct < 30) ownershipScore = 0.7;
    else if (effectiveOwnershipPct < 40) ownershipScore = 0.6;
  }
  
  // S/F Score (Shares Outstanding / Float ratio)
  // Higher ratio means more shares outstanding relative to float
  let sfScore = 0.5;
  const numFloat = parseFloat(float) || 1;
  const numShares = parseFloat(sharesOutstanding) || 1;
  const sfRatio = numShares / numFloat; // Shares Outstanding / Float

  if (sfRatio >= 5.0) sfScore = 1.0;      // 5x or more shares than float
  else if (sfRatio >= 3.0) sfScore = 0.9;
  else if (sfRatio >= 2.0) sfScore = 0.8;
  else if (sfRatio >= 1.5) sfScore = 0.7;
  else if (sfRatio >= 1.0) sfScore = 0.6;
  
  // Volume Score (higher volume ratio is better)
  let volumeScore = 0.5;
  const volumeRatio = avgVolume > 0 ? volume / avgVolume : 0.5;
  if (volumeRatio >= 3.0) volumeScore = 1.0;
  else if (volumeRatio >= 2.5) volumeScore = 0.9;
  else if (volumeRatio >= 2.0) volumeScore = 0.8;
  else if (volumeRatio >= 1.5) volumeScore = 0.7;
  else if (volumeRatio >= 1.0) volumeScore = 0.6;

  // Calculate signal score (multiply all 4 factors)
  const signalScore = floatScore * ownershipScore * sfScore * volumeScore;
  
  return {
    score: parseFloat(signalScore.toFixed(2)),
    floatScore: parseFloat(floatScore.toFixed(2)),
    ownershipScore: parseFloat(ownershipScore.toFixed(2)),
    sfScore: parseFloat(sfScore.toFixed(2)),
    volumeScore: parseFloat(volumeScore.toFixed(2))
  };
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
  'FDA Approval': ['FDA Approval', 'FDA Clearance', 'EMA Approval', 'Breakthrough Therapy', 'Fast Track Designation', 'Priority Review'],
  'Clinical Success': ['Positive Trial Results', 'Phase 3 Success', 'Topline Results Beat', 'Efficacy Demonstrated', 'Safety Profile Met'],
  'Capital Raise': ['Oversubscribed', 'Institutional Participation', 'Lead Investor', 'Top-Tier Investor', 'Strategic Investor'],
  'Earnings Beat': ['Earnings Beat', 'Beat Expectations', 'Beat Consensus', 'Exceeded Guidance', 'Record Revenue'],
  'Major Contract': ['Contract Award', 'Major Customer Win', 'Strategic Partnership', '$100 Million Contract', 'Exclusive License'],
  'Regulatory Approval': ['Regulatory Approval Granted', 'Patent Approved', 'License Granted', 'Permit Issued'],
  'Revenue Growth': ['Revenue Growth Acceleration', 'Record Quarterly Revenue', 'Guidance Raise', 'Organic Growth'],
  'Insider Buying': ['Director Purchase', 'Executive Purchase', 'CEO Buying', 'CFO Buying', 'Meaningful Accumulation'],
  
  'Reverse Split': ['Reverse Stock Split', 'Reverse Split', 'Reversed Split', 'Reverse Split Announced', 'Announced Reverse Split', '1-for-', 'Consolidation Of Shares', 'Share Consolidation', 'Combine Shares', 'Combined Shares', 'Restructuring Of Capital', 'Stock Consolidation', 'Share Combination'],
  'Bankruptcy Filing': ['Bankruptcy Protection', 'Chapter 11 Filing', 'Chapter 7 Filing', 'Insolvency Proceedings', 'Creditor Protection'],
  'Going Concern': ['Going Concern Warning', 'Substantial Doubt Going Concern', 'Auditor Going Concern Note', 'Continued Losses'],
  'Public Offering': ['Public Offering Announced', 'Secondary Offering', 'Follow-On Offering', 'Shelf Offering', 'At-The-Market Offering'],
  'Dilution': ['Dilutive Securities', 'New Shares Issued', 'Convertible Notes', 'Warrant Issuance', 'Option Grants Excessive'],
  'Delisting Risk': ['Nasdaq Deficiency', 'Listing Standards Warning', 'Nasdaq Notification', 'Minimum Bid Price Warning', 'Delisting Threat'],
  'Warrant Redemption': ['Warrant Redemption Notice', 'Forced Exercise', 'Warrant Call Notice', 'Final Expiration Notice'],
  'Insider Selling': ['Director Sale', 'Officer Sale', 'CEO Selling', 'CFO Selling', 'Massive Liquidation'],
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
  'Facility Expansion': ['New Facility Opening', 'Capacity Expansion', 'Manufacturing Expansion', 'Facility Upgrade']
};


const SEC_CODE_TO_COUNTRY = {'C2':'Shanghai, China','F4':'Shadong, China','6A':'Shanghai, China','D8':'Hong Kong','H0':'Hong Kong','K3':'Kowloon Bay, Hong Kong','S4':'Singapore','U0':'Singapore','C0':'Grand Cayman, Cayman Islands','K2':'Grand Cayman, Cayman Islands','E9':'Grand Cayman, Cayman Islands','1E':'Charlotte Amalie, U.S. Virgin Islands','VI':'Road Town, British Virgin Islands','A1':'Toronto, Canada','A6':'Ottawa, Canada','A9':'Vancouver, Canada','A0':'Calgary, Canada','CA':'Toronto, Canada','C4':'Toronto, Canada','D0':'Hamilton, Canada','D9':'Toronto, Canada','Q0':'Toronto, Canada','L3':'Tel Aviv, Israel','J1':'Tokyo, Japan','M0':'Tokyo, Japan','E5':'Dublin, Ireland','I0':'Dublin, Ireland','L2':'Dublin, Ireland','DE':'Wilmington, Delaware','1T':'Athens, Greece','B2':'Bridgetown, Barbados','B6':'Nassau, Bahamas','B9':'Hamilton, Bermuda','C1':'Buenos Aires, Argentina','C3':'Brisbane, Australia','C7':'St. Helier, Channel Islands','D2':'Hamilton, Bermuda','D4':'Hamilton, Bermuda','D5':'Sao Paulo, Brazil','D6':'Bridgetown, Barbados','E4':'Hamilton, Bermuda','F2':'Frankfurt, Germany','F3':'Paris, France','F5':'Johannesburg, South Africa','G0':'St. Helier, Jersey','G1':'St. Peter Port, Guernsey','G4':'New York, United States','G7':'Copenhagen, Denmark','H1':'St. Helier, Jersey','I1':'Douglas, Isle of Man','J0':'St. Helier, Jersey','J2':'St. Helier, Jersey','J3':'St. Helier, Jersey','K1':'Seoul, South Korea','K7':'New York, United States','L0':'Hamilton, Bermuda','L6':'Milan, Italy','M1':'Majuro, Marshall Islands','N0':'Amsterdam, Netherlands','N2':'Amsterdam, Netherlands','N4':'Amsterdam, Netherlands','O5':'Mexico City, Mexico','P0':'Lisbon, Portugal','P3':'Manila, Philippines','P7':'Madrid, Spain','P8':'Warsaw, Poland','R0':'Milan, Italy','S0':'Madrid, Spain','T0':'Lisbon, Portugal','T3':'Johannesburg, South Africa','U1':'London, United Kingdom','U5':'London, United Kingdom','V0':'Zurich, Switzerland','V8':'Geneva, Switzerland','W0':'Frankfurt, Germany','X0':'London, UK','X1':'Luxembourg City, Luxembourg','Y0':'Nicosia, Cyprus','Y1':'Nicosia, Cyprus','Z0':'Johannesburg, South Africa','Z1':'Johannesburg, South Africa','1A':'Pago Pago, American Samoa','1B':'Saipan, Northern Mariana Islands','1C':'Hagatna, Guam','1D':'San Juan, Puerto Rico','3A':'Sydney, Australia','4A':'Auckland, New Zealand','5A':'Apia, Samoa','7A':'Moscow, Russia','8A':'Mumbai, India','9A':'Jakarta, Indonesia','2M':'Frankfurt, Germany','U3':'Madrid, Spain','Y9':'Nicosia, Cyprus','AL':'Birmingham, UK','Q8':'Oslo, Norway','R1':'Panama City, Panama','V7':'Stockholm, Sweden','K8':'Jakarta, Indonesia','O9':'Monaco','W8':'Istanbul, Turkey','R5':'Lima, Peru','N8':'Kuala Lumpur, Malaysia'};

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
  // Default to NASDAQ for most US stocks
  // OTC stocks typically have 4-5 letters with some patterns
  // NYSE has different listing requirements
  
  if (!ticker || ticker === 'Unknown') return 'NASDAQ';
  
  // OTC indicators - typically longer tickers or specific patterns
  if (ticker.length > 4 || ticker.includes('OTC') || /[A-Z]{5}/.test(ticker)) {
    return 'OTC';
  }
  
  // For now, default to NASDAQ for 1-4 letter tickers
  // You can add specific NYSE mappings if needed
  return 'NASDAQ';
};

const truncateAtSentence = (text, maxLength) => {
  if (!text || text.length <= maxLength) return text;
  
  // Truncate to max length first
  let truncated = text.substring(0, maxLength);
  
  // Find last sentence boundary (. ! ?)
  const lastPeriod = Math.max(
    truncated.lastIndexOf('.'),
    truncated.lastIndexOf('!'),
    truncated.lastIndexOf('?')
  );
  
  // If we found punctuation, cut there and add it back
  if (lastPeriod > 0) {
    return truncated.substring(0, lastPeriod + 1);
  }
  
  // If no punctuation found, cut at last space to avoid mid-word cuts
  const lastSpace = truncated.lastIndexOf(' ');
  if (lastSpace > maxLength * 0.7) {
    return truncated.substring(0, lastSpace);
  }
  
  // Fallback to max length
  return truncated;
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
    
    // Cleanup stale alerts based on day of week
    cleanupStaleAlerts();
    
    if (Object.keys(alertData.semanticSignals || {}).length > 0) {
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
    
    log('INFO', `Log: Alert saved ${alertData.ticker} (pushed to GitHub)`);
    
    const volDisplay = alertData.volume && alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'n/a';
    const avgVolDisplay = alertData.averageVolume && alertData.averageVolume !== 'N/A' ? (alertData.averageVolume / 1000000).toFixed(2) + 'm' : 'n/a';
    const floatDisplay = alertData.float && alertData.float !== 'N/A' && !isNaN(alertData.float) ? (alertData.float / 1000000).toFixed(2) + 'm' : 'n/a';
    const soDisplay = alertData.soRatio || 'n/a';
    
    const priceDisplay = alertData.price && alertData.price !== 'N/A' ? `$${alertData.price.toFixed(2)}` : 'N/A';
    const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${alertData.cik}&type=${alertData.formType}&dateb=&owner=exclude&count=100`;
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

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchFilings() {
  const allFilings = [];
  
  try {
    await wait(200);
    const res = await fetch('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=6-K&count=100&owner=exclude&output=atom', {
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
    log('WARN', `SEC 6-K fetch failed: ${err.message}`);
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
    'GB': 'United Kingdom', 'CH': 'Switzerland', 'DE': 'Germany', 'FR': 'France', 'BR': 'Brazil'
  };
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const padded = cik.toString().padStart(10, '0');
      await wait(500);
      const res = await fetch(`https://data.sec.gov/submissions/CIK${padded}.json`, {
        headers: {
          'User-Agent': 'SEC-Bot/1.0 (your-email@domain.com)',
          'Accept': 'application/json'
        },
        timeout: CONFIG.SEC_FETCH_TIMEOUT
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
    const res = await fetch('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=100&owner=exclude&output=atom', {
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
    log('WARN', `SEC 8-K fetch failed: ${err.message}`);
  }
  return filings8K;
}

async function getFilingText(indexUrl) {
  for (let attempt = 1; attempt <= CONFIG.MAX_RETRY_ATTEMPTS; attempt++) {
    try {
      await wait(1000); // 1 second delay before SEC request
      const res = await fetch(indexUrl, {
        headers: {
          'User-Agent': 'SEC-Bot/1.0 (your-email@domain.com)',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
          'Accept-Encoding': 'gzip, deflate, br',
          'Connection': 'keep-alive'
        }
      });

      if (res.status === 403) {
        log('WARN', `SEC blocked request (403), waiting 5 seconds`);
        await wait(5000);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      
      const html = await res.text();
      
      const docHrefs = [];
      
      const exhibitRegex = /href="([^"]*(?:ex|form|document)[^"]*\.(?:htm|html|txt))"/gi;
      let match;
      while ((match = exhibitRegex.exec(html)) !== null) {
        if (!match[1].toLowerCase().includes('index')) {
          docHrefs.push(match[1]);
        }
      }
      
      if (docHrefs.length === 0) {
        const fallbackRegex = /href="([^"]+\.(?:htm|html|txt))"/gi;
        while ((match = fallbackRegex.exec(html)) !== null) {
          if (!match[1].toLowerCase().includes('index') && !match[1].toLowerCase().includes('style')) {
            docHrefs.push(match[1]);
          }
        }
      }
      
      const txtFiles = docHrefs.filter(href => href.toLowerCase().endsWith('.txt'));
      const htmlFiles = docHrefs.filter(href => !href.toLowerCase().endsWith('.txt'));
      
      const prioritizedHrefs = txtFiles.length > 0 ? txtFiles : htmlFiles;
      if (prioritizedHrefs.length === 0) throw new Error(`No filing documents found at ${indexUrl}`);
      
      let combinedText = '';
      const MAX_COMBINED_SIZE = CONFIG.MAX_COMBINED_SIZE;
      
      for (const href of prioritizedHrefs) {
        const fullUrl = href.startsWith('http') ? href : `https://www.sec.gov${href}`;        
        
        try {
          const docRes = await fetch(fullUrl, {
            headers: {
              'User-Agent': 'SEC-Bot/1.0 (your-email@domain.com)',
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
              'Accept-Language': 'en-US,en;q=0.5',
              'Accept-Encoding': 'gzip, deflate, br',
              'Connection': 'keep-alive'
            }
          });

          let docText = await docRes.text();
          
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
            .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
            .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
            .replace(/<!--[\s\S]*?-->/g, '')
            .replace(/&nbsp;/g, ' ')
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&#(\d+);/g, (match, code) => String.fromCharCode(parseInt(code)))
            .replace(/<[^>]+>/g, ' ')
            // Strip SEC metadata patterns like "0001292814-25-004426.txt : 20251230 0001292814-25-004426.hdr"
            .replace(/\d{10}-\d{2}-\d{6}\.\w+\s*:\s*\d+\s*\d{10}-\d{2}-\d{6}\.\w+/g, '')
            // Remove exhibit/item/form references (already logged separately) - but be selective
            .replace(/^\s*(?:exhibit|annex|appendix|schedule|form|section)\s+[a-z0-9]+\s*\n/gim, '')
            // Remove common SEC boilerplate headers
            .replace(/(?:table of contents|index to exhibits|signatures|certification|forward-looking statements|risk factors)/gi, '')
            // Remove footer/header patterns
            .replace(/(?:page \d+|continued|see page|see exhibit|see schedule)/gi, '')
            // Remove date/time/filing metadata (but keep actual content dates)
            .replace(/(?:^\s*)*(?:filed (?:on |with )?|as of |as amended|amended and restated|effective (?:as of )?|period ended|fiscal year|calendar year|quarterly period)\s+/gim, '')
            // Remove SEC references
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
      log('ERROR', `Filing text fetch attempt ${attempt}/5 failed: ${err.message}`);
      if (attempt < 5) await wait(10000); // 10 seconds wait between retries
    }
  }
  log('ERROR', `Failed to fetch filing text after 5 attempts from ${indexUrl}`);
  return '';
}

const sendPersonalWebhook = (alertData) => {
  try {
    const { ticker, price, intent, incorporated, located } = alertData;
    
    const combinedLocation = (incorporated || '').toLowerCase() + ' ' + (located || '').toLowerCase();
    
    const allowed = CONFIG.ALLOWED_COUNTRIES.some(country => combinedLocation.includes(country));
    if (!allowed) {
      console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, country not whitelisted (${incorporated}, ${located})\x1b[0m`);
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
    
    const direction = intent?.toLowerCase().includes('short') || intent?.toLowerCase().includes('bankruptcy') || intent?.toLowerCase().includes('dilution') ? 'Short' : 'Long';
    const reason = (intent || 'Filing').substring(0, 50).toLowerCase();
    const priceDisplay = price && price !== 'N/A' ? `$${parseFloat(price).toFixed(2)}` : 'N/A';
    const volDisplay = alertData.volume && alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'N/A';
    const floatDisplay = alertData.float && alertData.float !== 'N/A' ? (alertData.float / 1000000).toFixed(2) + 'm' : 'N/A';
    const insiderDisplay = alertData.insiderPercent && alertData.insiderPercent !== 'N/A' ? (parseFloat(alertData.insiderPercent) * 100).toFixed(2) + '%' : 'N/A';
    const institutionalDisplay = alertData.institutionalPercent && alertData.institutionalPercent !== 'N/A' ? (parseFloat(alertData.institutionalPercent) * 100).toFixed(2) + '%' : 'N/A';
    const signalScoreDisplay = alertData.signalScore ? `**${alertData.signalScore}**` : 'N/A';
    
    // Build ownership display (prefer insider, fallback to institutional)
    const ownershipDisplay = insiderDisplay !== 'N/A' ? `ownership: ${insiderDisplay}` : `institutional: ${institutionalDisplay}`;
    
    const personalAlertContent = `↳ [${direction}] **$${ticker}** @ ${priceDisplay} (${countryDisplay}), score: ${signalScoreDisplay}, vol: ${volDisplay}, float: ${floatDisplay}, s/o: ${alertData.soRatio}, ownership: ${ownershipDisplay}, ${reason}, ${alertData.shortOpportunity || alertData.longOpportunity || ''} https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
    const personalMsg = { content: personalAlertContent };
    
    log('INFO', `Alert: [${direction}] $${ticker} @ ${priceDisplay}, Score: ${signalScoreDisplay}, Float: ${alertData.float !== 'N/A' ? (alertData.float / 1000000).toFixed(2) + 'm' : 'N/A'}, Vol: ${alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'N/A'}, S/O: ${alertData.soRatio}, ${ownershipDisplay}`);
    
    fetch(CONFIG.PERSONAL_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(personalMsg)
    }).catch(err => log('WARN', `Webhook send failed: ${err.message}`));
  } catch (err) {
    log('WARN', `Error sending personal webhook: ${err.message}`);
  }
};

const pushToGitHub = () => {
  try {
    const projectRoot = CONFIG.GITHUB_REPO_PATH;
    execSync(`cd ${projectRoot} && git add alert.json quote.json 2>/dev/null && git commit -m "Auto: Alert update $(date '+%Y-%m-%d %H:%M:%S')" 2>/dev/null && git push origin main 2>/dev/null`, { 
      stdio: 'ignore' 
    });
    log('INFO', `Pushed to GitHub (${CONFIG.GITHUB_USERNAME}/${CONFIG.GITHUB_REPO_NAME})`);
  } catch (err) {
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
    
    log('INFO', `Git: Branch ${branch || 'main'}, Last commit: ${lastCommit || 'No commits'}`);
    
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

// Quote endpoint with Yahoo fallback to Finnhub
app.get('/api/quote/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  
  try {
    // Try Yahoo Finance first
    let quote = await yahooFinance.quote(ticker, {
      fields: ['regularMarketPrice', 'regularMarketVolume', 'marketCap', 'exchange'],
      timeout: CONFIG.YAHOO_TIMEOUT
    }).catch(() => null);
    
    // If Yahoo fails, try Finnhub
    if (!quote || !quote.regularMarketPrice) {
      const finnhubKey = process.env.FINNHUB_API_KEY;
      if (finnhubKey) {
        try {
          const finnhubRes = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${finnhubKey}`, {
            timeout: 5000
          });
          if (finnhubRes.ok) {
            const data = await finnhubRes.json();
            // Finnhub data structure: c=current, v=volume
            if (data.c && data.c > 0) {
              quote = {
                symbol: ticker,
                regularMarketPrice: data.c,
                regularMarketVolume: data.v || 0,
                marketCap: 'N/A',
                exchange: 'UNKNOWN'
              };
            }
          }
        } catch (e) {
          // Silently fail Finnhub fallback
        }
      }
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
            insiderPercent: latestAlert.insiderPercent || 'N/A',
            soRatio: latestAlert.soRatio || 'N/A'
          };
        }
      }
    } catch (e) {
      // Silently fail if alert.json doesn't exist
    }
    
    res.json({
      symbol: ticker,
      price: quote?.regularMarketPrice || 'N/A',
      volume: quote?.regularMarketVolume || 0,
      averageVolume: quote?.regularMarketVolume || 0,
      marketCap: quote?.marketCap || 'N/A',
      exchange: quote?.exchange || 'UNKNOWN',
      float: fundamentals.float || 'N/A',
      sharesOutstanding: fundamentals.sharesOutstanding || 'N/A',
      insiderPercent: fundamentals.insiderPercent || 'N/A',
      soRatio: fundamentals.soRatio || 'N/A'
    });
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
      insiderPercent: 'N/A',
      soRatio: 'N/A'
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
});

(async () => {
  let cycleCount = 0, alertsSent = 0, startTime = Date.now();
  let processedHashes = new Map(); // Pure in-memory, session-based (100 max)
  let loggedFetch = false;
  
  log('INFO', `App: System online at http://localhost:${PORT} & https://eugenesnonprofit.com/`);
  
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
      const filings6K = await fetchFilings();
      const filings8K = await fetch8Ks();
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
      for (const filing of filingsToProcess) {
        try {
          const hash = crypto.createHash('md5').update(filing.title + filing.updated).digest('hex');
          
          if (processedHashes.has(hash)) {
            continue;
          }
          
          processedHashes.set(hash, Date.now());
          
          const filingTime = new Date(filing.updated);
          const filingDate = filingTime.toLocaleString('en-US', { timeZone: 'America/New_York' });
          const text = await getFilingText(filing.txtLink);
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
                newsDisplay = newsDisplay.replace(/1-for-/i, ratio + ' ');
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
          
          const mainForms = ['S-1', 'S-3', 'S-4', 'S-8', 'F-1', 'F-3', 'F-4', '20-F', '13G', '13D', 'Form D'];
          const mainItems = ['1.01', '1.02', '1.03', '1.04', '1.05', '2.01', '2.02', '2.03', '2.04', '2.05', '2.06', '3.01', '3.02', '3.03', '4.01', '5.01', '5.02', '5.03', '5.04', '5.05', '5.06', '5.07', '5.08', '5.09', '5.10', '5.11', '5.12', '5.13', '5.14', '5.15', '6.01', '7.01', '8.01', '9.01'];
          const otherForms = Array.from(foundForms).filter(f => mainForms.includes(f));
          const otherItems = Array.from(foundItems).filter(i => mainItems.includes(i));
          const formsDisplay = otherForms.length > 0 ? otherForms.join(', ') : '';
          const itemsDisplay = otherItems.length > 0 ? otherItems.sort((a, b) => parseFloat(a) - parseFloat(b)).map(item => `Item ${item}`).join(', ') : '';
          
          const bearishCategories = ['Reverse Split', 'Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Dilution', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis'];
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
          log('INFO', `Form: ${formLogMessage}`);
          
          if (signalKeys.length > 0) {
            log('INFO', `${direction}: ${signalKeys.join(', ')}`);
          }
          
          let price = 'N/A', volume = 'N/A', marketCap = 'N/A', averageVolume = 'N/A', float = 'N/A', sharesOutstanding = 'N/A', insiderPercent = 'N/A', institutionalPercent = 'N/A';
          let debtToEquity = 'N/A', totalCash = 'N/A', totalCashPerShare = 'N/A', profitMargins = 'N/A', quickRatio = 'N/A';
          
          if (ticker !== 'UNKNOWN' && isValidTicker(ticker)) {
            try {
              const quoteData = await Promise.race([
                yahooFinance.quoteSummary(ticker, { modules: ['price', 'summaryDetail', 'defaultKeyStatistics', 'majorHoldersBreakdown', 'financialData'] }),
                new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.YAHOO_TIMEOUT))
              ]);
              
              const priceModule = quoteData.price || {};
              const summaryDetail = quoteData.summaryDetail || {};
              const keyStats = quoteData.defaultKeyStatistics || {};
              const holders = quoteData.majorHoldersBreakdown || {};
              const finData = quoteData.financialData || {};
              
              price = priceModule?.regularMarketPrice || summaryDetail?.regularMarketPrice || 'N/A';
              marketCap = summaryDetail?.marketCap || 'N/A';
              volume = priceModule?.regularMarketVolume || summaryDetail?.regularMarketVolume || 'N/A';
              averageVolume = summaryDetail?.averageVolume || summaryDetail?.averageDailyVolume10Day || 'N/A';
              float = keyStats?.floatShares || 'N/A';
              sharesOutstanding = keyStats?.sharesOutstanding || 'N/A';
              
              // Yahoo returns insider/institutional as decimals (0-1), convert to percentages
              insiderPercent = holders?.insidersPercentHeld ? (holders.insidersPercentHeld * 100) : 'N/A';
              institutionalPercent = holders?.institutionsPercentHeld ? (holders.institutionsPercentHeld * 100) : 'N/A';
              
              debtToEquity = keyStats?.debtToEquity || 'N/A';
              totalCash = finData?.totalCash || 'N/A';
              totalCashPerShare = finData?.totalCashPerShare || 'N/A';
              profitMargins = keyStats?.profitMargins || 'N/A';
              quickRatio = finData?.quickRatio || 'N/A';
            } catch (yahooErr) {
              // Yahoo failed, try Finnhub fallback for comprehensive data
              const finnhubKey = process.env.FINNHUB_API_KEY;
              if (finnhubKey) {
                try {
                  // Get quote data (price, volume, change, changePercent)
                  const finnhubRes = await Promise.race([
                    fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${finnhubKey}`),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                  ]);
                  if (finnhubRes.ok) {
                    const data = await finnhubRes.json();
                    if (data.c && data.c > 0 && price === 'N/A') {
                      price = data.c;
                    }
                    if (data.v && volume === 'N/A') {
                      volume = data.v;
                    }
                  }
                  
                  // Get company profile for market cap, shares outstanding, and IPO date
                  const profileRes = await Promise.race([
                    fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                  ]);
                  if (profileRes.ok) {
                    const profileData = await profileRes.json();
                    if (profileData.marketCapitalization && marketCap === 'N/A') {
                      marketCap = profileData.marketCapitalization * 1000000;
                    }
                    if (profileData.shareOutstanding && sharesOutstanding === 'N/A') {
                      sharesOutstanding = profileData.shareOutstanding;
                    }
                  }
                  
                  // Get basic financials for additional metrics (volumes, ratios, cash data)
                  const metricsRes = await Promise.race([
                    fetch(`https://finnhub.io/api/v1/stock/metric?symbol=${ticker}&metric=all&token=${finnhubKey}`),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                  ]);
                  if (metricsRes.ok) {
                    const metricsData = await metricsRes.json();
                    const metrics = metricsData.metric || {};
                    
                    // Average volume
                    if (metrics['10DayAverageTradingVolume'] && averageVolume === 'N/A') {
                      averageVolume = metrics['10DayAverageTradingVolume'] * 1000000;
                    }
                    
                    // Note: Finnhub doesn't directly provide float, debtToEquity, totalCash, totalCashPerShare, profitMargins, or quickRatio
                    // These would require additional premium endpoints or calculations
                  }
                } catch (finnhubErr) {
                  // Finnhub also failed, keep N/A values
                }
              }
            }
          }
          
          const priceDisplay = price !== 'N/A' ? `$${price.toFixed(2)}` : 'N/A';
          const volDisplay = volume !== 'N/A' ? volume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const avgDisplay = averageVolume !== 'N/A' ? averageVolume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const mcDisplay = marketCap !== 'N/A' && marketCap > 0 ? '$' + Math.round(marketCap).toLocaleString('en-US') : 'N/A';
          const floatDisplay = float !== 'N/A' ? float.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const insiderDisplay = insiderPercent !== 'N/A' ? (insiderPercent * 100).toFixed(2) + '%' : 'N/A';
          let soRatio = 'N/A';
          if (sharesOutstanding !== 'N/A' && float !== 'N/A' && sharesOutstanding > 0 && !isNaN(float) && !isNaN(sharesOutstanding)) {
            const ratio = (float / sharesOutstanding) * 100;
            soRatio = ratio < 100 ? ratio.toFixed(2) + '%' : ratio.toFixed(1) + '%';
          }
          
          let shortOpportunity = null;
          let longOpportunity = null;
          
          if ((normalizedIncorporated.includes('Cayman') || normalizedIncorporated.includes('Virgin') || normalizedIncorporated.includes('Hong Kong')) 
              && insiderPercent !== 'N/A' && insiderPercent < 0.01 
              && Object.keys(semanticSignals).includes('Reverse Split')) {
            shortOpportunity = 'Intent: Artificial Inflation (Foreign Issuer + Low Insider + Reverse Split)';
          }
          
          if (profitMargins !== 'N/A' && profitMargins < -0.5 
              && totalCashPerShare !== 'N/A' && totalCashPerShare < 0.05 
              && Object.keys(semanticSignals).includes('Dilution')) {
            shortOpportunity = 'Intent: Desperation Play (Negative Margins + Low Cash + Dilution)';
          }
          
          if (debtToEquity !== 'N/A' && debtToEquity > 5000 
              && insiderPercent !== 'N/A' && insiderPercent < 0.01) {
            shortOpportunity = 'Intent: Bankruptcy Play (High Debt + No Insider Support)';
          }
          
          if (Object.keys(semanticSignals).includes('Going Concern') 
              && quickRatio !== 'N/A' && quickRatio < 0.5) {
            shortOpportunity = 'Intent: Imminent Collapse (Going Concern + Insolvency)';
          }
          
          if (insiderPercent !== 'N/A' && insiderPercent < 0.01 
              && totalCashPerShare !== 'N/A' && totalCashPerShare < 0.05 
              && profitMargins !== 'N/A' && profitMargins < 0) {
            shortOpportunity = 'Intent: Dilution Incoming (Insider Exit + Cash Burn)';
          }
          
          if (insiderPercent !== 'N/A' && insiderPercent > 0.15 
              && profitMargins !== 'N/A' && profitMargins > 0.1 
              && (Object.keys(semanticSignals).includes('Merger/Acquisition') || Object.keys(semanticSignals).includes('FDA Approval'))) {
            longOpportunity = 'Intent: Insider Support + Strong Fundamentals';
          }
          
          const now = new Date();
          const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
          const etHour = etTime.getHours();
          const etMin = etTime.getMinutes();
          const etTotalMin = etHour * 60 + etMin;
          const startMin = 3.5 * 60; // 3:30am = 210 minutes
          const endMin = 18 * 60; // 6:00pm = 1080 minutes
          
          if (shortOpportunity || longOpportunity) {
            log('INFO', `Stock: $${ticker}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}, Ownership: ${insiderDisplay}, ${shortOpportunity || longOpportunity}`);
          } else {
            log('INFO', `Stock: $${ticker}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}, Ownership: ${insiderDisplay}`);
          }
          
          if (etTotalMin < startMin || etTotalMin > endMin) {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, filing received at ${etHour.toString().padStart(2, '0')}:${etMin.toString().padStart(2, '0')} ET (outside 3:30am-6:00pm window)\x1b[0m`);
            console.log('');
            continue;
          }
          if (normalizedLocated === 'Unknown') {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, no valid country\x1b[0m`);
            console.log('');
            continue;
          }
          
          const floatValue = float !== 'N/A' ? parseFloat(float) : null;
          if (floatValue !== null && floatValue > CONFIG.MAX_FLOAT) {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, Float ${floatValue.toLocaleString('en-US')} exceeds ${(CONFIG.MAX_FLOAT / 1000000).toFixed(0)}m limit\x1b[0m`);
            console.log('');
            continue;
          }
          
          const volumeValue = volume !== 'N/A' ? parseFloat(volume) : null;
          if (volumeValue !== null && volumeValue < CONFIG.MIN_ALERT_VOLUME) {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, volume ${volumeValue.toLocaleString('en-US')} below ${(CONFIG.MIN_ALERT_VOLUME / 1000).toFixed(0)}k minimum\x1b[0m`);
            console.log('');
            continue;
          }
          
          let soRatioValue = null;
          if (sharesOutstanding !== 'N/A' && float !== 'N/A' && sharesOutstanding > 0) {
            const so = parseFloat(sharesOutstanding);
            const fl = parseFloat(float);
            if (!isNaN(fl) && !isNaN(so)) {
              soRatioValue = (fl / so) * 100;
            }
          }
          
          if (soRatioValue !== null && soRatioValue >= CONFIG.MAX_SO_RATIO) {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, S/O ${soRatioValue.toFixed(2)}% exceeds ${CONFIG.MAX_SO_RATIO.toFixed(0)}% limit\x1b[0m`);
            console.log('');
            continue;
          }
          
          const neutralCategories = ['Executive Departure', 'Asset Impairment', 'Restructuring', 'Stock Buyback', 'Licensing Deal', 'Partnership', 'Facility Expansion'];
          const signalCategories = Object.keys(semanticSignals);
          const neutralSignals = signalCategories.filter(cat => neutralCategories.includes(cat));
          const nonNeutralSignals = signalCategories.filter(cat => !neutralCategories.includes(cat));
          
          let validSignals = false;
          if (neutralSignals.length > 0 && signalCategories.length >= 2) {
            validSignals = true; // Has neutral signal + at least 1 other signal
          } else if (nonNeutralSignals.length >= 2) {
            validSignals = true; // Has 2+ non-neutral signals from different categories
          }
          
          if (!validSignals) {
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=${filing.formType}&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            console.log(`\x1b[90m[${new Date().toISOString()}]\x1b[0m \x1b[31mSKIP: $${ticker}, not enough signals (needs 2 plus or 1 neutral and a bearish/bullish)\x1b[0m`);
            console.log('');
            continue;
          }

          const alertData = {
            ticker: ticker || filing.cik || 'Unknown',
            price: price,
            volume: volume,
            averageVolume: averageVolume,
            float: float,
            sharesOutstanding: sharesOutstanding,
            soRatio: soRatio,
            insiderPercent: insiderPercent,
            marketCap: marketCap,
            shortOpportunity: shortOpportunity,
            floatFilter: floatValue !== null ? (floatValue <= 25000000 ? 'Pass' : 'Fail') : 'N/A',
            volumeFilter: volumeValue !== null ? (volumeValue >= 50000 ? 'Pass' : 'Fail') : 'N/A',
            soFilter: soRatioValue !== null ? (soRatioValue < 25 ? 'Pass' : 'Fail') : 'N/A',
            intent: intent || 'Regulatory Filing',
            incorporated: normalizedIncorporated,
            located: normalizedLocated,
            semanticSignals: semanticSignals,
            filingDate: periodOfReport,
            source: source,
            formType: foundForms,
            cik: filing.cik
          };
          
          // Calculate signal score
          const numFloat = typeof float === 'number' ? float : parseInt(float) || 0;
          const numVolume = typeof volume === 'number' ? volume : parseInt(volume) || 0;
          const numAvgVol = typeof averageVolume === 'number' ? averageVolume : parseInt(averageVolume) || 1;
          const numInsider = typeof insiderPercent === 'number' ? insiderPercent * 100 : (typeof insiderPercent === 'string' ? parseFloat(insiderPercent) : 0);
          const numInstitutional = typeof institutionalPercent === 'number' ? institutionalPercent * 100 : (typeof institutionalPercent === 'string' ? parseFloat(institutionalPercent) : 0);
          const numShares = typeof sharesOutstanding === 'number' ? sharesOutstanding : parseInt(sharesOutstanding) || 1;
          
          const signalScoreData = calculatesignalScore(numFloat, numInsider, numInstitutional, numShares, numVolume, numAvgVol);
          alertData.signalScore = signalScoreData.score;
          alertData.institutionalPercent = institutionalPercent;
          alertData.scoreBreakdown = {
            floatScore: signalScoreData.floatScore,
            ownershipScore: signalScoreData.ownershipScore,
            sfScore: signalScoreData.sfScore,
            volumeScore: signalScoreData.volumeScore
          };
          
          // Log the signal score
          log('INFO', `Score: ${signalScoreData.score} (F: ${signalScoreData.floatScore}, O: ${signalScoreData.ownershipScore}, S/F: ${signalScoreData.sfScore}, V: ${signalScoreData.volumeScore})`);
          
          // Only save alert if we got price, float, S/O, and ownership data
          if (price !== 'N/A' && float !== 'N/A' && soRatio !== 'N/A' && insiderPercent !== 'N/A' && institutionalPercent !== 'N/A') {
            saveAlert(alertData);
          } else {
            log('INFO', `Quote: Incomplete data for ${ticker} (price: ${price}, float: ${float}, s/o: ${soRatio}, ownership: ${insiderPercent === 'N/A' ? institutionalPercent : insiderPercent})`);
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
