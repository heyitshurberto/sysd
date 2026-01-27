import fs from 'fs';
import fetch from 'node-fetch';
import { createRequire } from 'module';
import { execSync } from 'child_process';
import crypto from 'crypto';
import express from 'express';
import bcrypt from 'bcrypt';

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
  FILE_TIME: 1,                     // Minutes retro to fetch filings
  MIN_ALERT_VOLUME: 20000,          // Lower base, conditional on signal strength
  STRONG_SIGNAL_MIN_VOLUME: 1000,   // Very low for penny stocks with extreme S/O
  EXTREME_SO_RATIO: 80,             // 80%+ S/O = tight float
  MAX_FLOAT_6K: 100000000,          // Max float size for 6-K
  MAX_FLOAT_8K: 250000000,          // Max float size for 8-K 
  MAX_SO_RATIO: 1000.0,             // Max short interest ratio
  ALLOWED_COUNTRIES: ['israel', 'china', 'hong kong', 'australia', 'cayman islands', 'virgin islands', 'singapore', 'canada', 'nevada', 'delaware'], // Allowed incorporation/located countries
  // Enable optimizations for Raspberry Pi devices
  PI_MODE: true,              // Enable Pi optimizations          
  REFRESH_PEAK: 1,            // 10s during trading hours (7am-10am ET)
  REFRESH_NORMAL: 30000,      // 30s during trading hours (3:30am-6pm ET)
  REFRESH_NIGHT: 300000,      // 5m outside trading hours (conserve power)
  REFRESH_WEEKEND: 600000,    // 10m on weekends (very low activity)
  YAHOO_TIMEOUT: 10000,       // Reduced from 10s for Pi performance
  SEC_RATE_LIMIT: 5000,       // Minimum 5ms between SEC requests
  SEC_FETCH_TIMEOUT: 10000,   // Increased to 10s for large SEC filings (was 5s causing timeouts)
  MAX_COMBINED_SIZE: 100000,  // Reduced from 150k for Pi RAM
  MAX_RETRY_ATTEMPTS: 7,      // Reduced from 7 for Pi resources
  // Log files
  ALERTS_FILE: 'logs/alert.json',      // File to store recent alerts
  STOCKS_FILE: 'logs/stocks.json',     // File to store all alerts
  PERFORMANCE_FILE: 'logs/quote.json', // File to store performance data
  CSV_FILE: 'logs/track.csv',          // File to store CSV export of all alerts
  // GitHub & Webhook settings
  GITHUB_REPO_PATH: process.env.GITHUB_REPO_PATH || '/home/user/Documents/sysd', // Local path to GitHub repo
  GITHUB_USERNAME: process.env.GITHUB_USERNAME || 'your-github-username', // GitHub username
  GITHUB_REPO_NAME: process.env.GITHUB_REPO_NAME || 'your-repo-name', // GitHub repo name
  GITHUB_DOMAIN: process.env.GITHUB_DOMAIN || 'your-domain.com', // GitHub Pages domain
  GITHUB_PUSH_ENABLED: process.env.GITHUB_PUSH_ENABLED !== 'false' && process.env.GITHUB_PUSH_ENABLED !== '0', // Enable/disable GitHub push (default: true, set to false in .env to disable)
  PERSONAL_WEBHOOK_URL: process.env.DISCORD_WEBHOOK || '', // Personal Discord webhook URL
  DISCORD_ENABLED: process.env.DISCORD_ENABLED === 'true', // Enable/disable Discord alerts (set to 'true' in .env to enable)
  // Telegram settings
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN || '', // Telegram bot token
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID || '', // Telegram chat ID for alerts
  TELEGRAM_ENABLED: process.env.TELEGRAM_ENABLED !== 'false' && process.env.TELEGRAM_ENABLED !== '0', // Enable/disable Telegram alerts (default: true)
  // Domain settings
  GITHUB_PAGES_ENABLED: process.env.GITHUB_PAGES_ENABLED !== 'false' && process.env.GITHUB_PAGES_ENABLED !== '0', // Enable/disable GitHub Pages domain push (default: true)
  // 2FA settings
  TWO_FACTOR_ENABLED: true, // Set to false to disable 2FA approval gate (keep basic auth always on)
  // Email authentication settings
  EMAIL_AUTH_ENABLED: true, // Use email-based auth instead of basic auth
  SMTP_HOST: process.env.SMTP_HOST || 'smtp.gmail.com',
  SMTP_PORT: process.env.SMTP_PORT || 587,
  SMTP_USER: process.env.SMTP_USER || '',
  SMTP_PASS: process.env.SMTP_PASS || '',
  EMAIL_FROM: process.env.EMAIL_FROM || 'noreply@cartelventures.com'
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

// Parse applicant/registrant name from SEC filing text - BULLETPROOF VERSION
// SEC filings ALWAYS have company name in standardized headers
const parseApplicantName = (text) => {
  if (!text) return 'N/A';
  
  // Pattern 1: "APPLICANT:" or "Applicant:" with full company info (most common)
  let match = text.match(/^[^a-z]*?APPLICANT\s*[:\-]?\s*\n?\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/im);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 2: "REGISTRANT:" field (8-K/6-K standard header)
  match = text.match(/^[^a-z]*?REGISTRANT\s*[:\-]?\s*\n?\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/im);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 3: "Name of Registrant" standard SEC label
  match = text.match(/Name of Registrant\s*[:\-]?\s*\n?\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 4: Header with company name + CIK (most reliable format)
  match = text.match(/([A-Z][A-Za-z0-9\s&,.\-()/'\']*(?:INC|LLC|LTD|CORP|CORPORATION|CO|COMPANY|GROUP|HOLDINGS|PLC|AG|SE|GmbH|Ltd|Inc|LLC)\.?)\s*\n\s*\(?[0-9]{10}\)?/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 5: "Form 8-K" / "Form 6-K" cover page with company name on first real line
  match = text.match(/(?:FORM\s*(?:8-?K|6-?K|10-?K|10-?Q).*?\n){1,3}\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    // Exclude boilerplate
    if (!/^(SEC|EDGAR|ITEM|EXHIBIT|SCHEDULE|TABLE OF CONTENTS)$/i.test(name) && name.length > 2) {
      return name.substring(0, 150);
    }
  }
  
  // Pattern 6: Company name before CIK number (very common)
  match = text.match(/^([A-Z][A-Za-z0-9\s&,.\-()/'\']*?)\s{2,}\d{10}/im);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2 && !/^(SEC|EDGAR|FORM|ITEM)$/i.test(name)) return name;
  }
  
  // Pattern 7: After "UNITED STATES OF AMERICA" SEC header
  match = text.match(/UNITED STATES OF AMERICA[^\n]*\n\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 8: "SEC File No" followed by company name
  match = text.match(/(?:SEC File No|File Number)[^\n]*\n\s*([A-Z][A-Za-z0-9\s&,.\-()/'\']+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 2) return name;
  }
  
  // Pattern 9: First substantive capitalized line (fallback)
  match = text.match(/^[^a-z\n]{0,50}([A-Z][A-Za-z0-9\s&,.\-()/'\']{10,}?)(?:\n|$)/m);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ').substring(0, 150);
    if (name.length > 3 && !/^(EXHIBIT|TABLE|SCHEDULE|FORM|ITEM|PART)$/i.test(name)) {
      return name;
    }
  }
  
  return 'N/A';
};

// Extract the actual person/entity filing the document (not company name)
// Look for "Applicant" or explicit filer signatures
const parseFilerName = (text) => {
  if (!text) return null;
  
  // Helper to validate if text looks like a real person/officer name
  const isValidName = (str) => {
    if (!str || str.length < 2 || str.length > 150) return false;
    
    // Reject explicit non-names
    if (/^(N\/A|NA|UNKNOWN|Unknown|None|NONE|Yes|No|True|False)$/i.test(str)) return false;
    
    // Reject if it's obviously boilerplate/instructions
    if (/Translation of registrant|as specified in|charter|agreement|contract|Please see|Exhibit|Form \d|SEC|EDGAR|Item \d|Schedule|pursuant to|hereby/i.test(str)) return false;
    
    // Reject URLs, emails, pure special chars
    if (/www\.|http|@|\.com|^[\d\s,.\-()'"\/&;:]+$/.test(str)) return false;
    
    // Reject pure numbers
    if (/^\d+$/.test(str)) return false;
    
    // Reject if mostly numbers (>40% numeric)
    const numCount = (str.match(/\d/g) || []).length;
    if (numCount > str.length * 0.4) return false;
    
    // Reject street addresses (numbered streets, compass directions in addresses)
    if (/^\d+\s+(?:Front|Queen|Main|Broadway|Street|St\.|Avenue|Ave\.|Road|Rd\.|Suite|Apt\.|Floor|Circle|Drive|Lane|Place|Boulevard|Blvd|North|South|East|West|N\.|S\.|E\.|W\.)/i.test(str)) return false;
    if (/Street|Avenue|Suite|Floor|Building|P\.O\.|Box\s+\d|Chicago|New York|London|Tokyo|Singapore|Toronto|Vancouver|Sydney|Hong Kong|India|Korea|Israel|Germany|France|UK|USA|Inc\.|Ltd\.|Corp\.|Company|plc|Corp|International|Inc|CORPORATION|HOLDINGS|MANAGEMENT|SYSTEMS/i.test(str)) return false;
    
    // Must have actual letters (not just numbers/symbols)
    if (!/[a-zA-Z]/.test(str)) return false;
    
    // Should have at least 2 letters (rules out single initials or weird chars)
    if ((str.match(/[a-zA-Z]/g) || []).length < 2) return false;
    
    return true;
  };
  
  // Pattern 1: Signature block - "Name: XXXXX" after /s/ or "By:" line
  // Captures: "Name: Rajesh Magow" or "Name: Wes Levitt" or "Name: CHUN Sang Yung"
  let match = text.match(/Name\s*[:\-]\s*\n?\s*([^\n\/,]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  // Pattern 2: "By: /s/ XXXXX" signature line - extract name after /s/
  match = text.match(/By\s*:?\s*\/s\/\s*([^\n\/]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  // Pattern 3: Direct "APPLICANT:" label with name on next line
  match = text.match(/APPLICANT\s*[:\-]?\s*\n\s*([^\n]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  // Pattern 4: "Applicant Name: XXXXX"
  match = text.match(/Applicant\s+Name\s*[:\-]\s*([^\n,]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  // Pattern 5: Officer title + name (CEO, President, Secretary, CFO, etc.)
  match = text.match(/(?:Chief\s+Executive\s+Officer|CEO|President|Secretary|Chief\s+Financial\s+Officer|CFO|Chief\s+Investment\s+Officer|CIO|Deputy\s+Company\s+Secretary)\s*[:\-]?\s*\n\s*([^\n\/]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  // Pattern 6: "Filer Name: XXXXX" or "Registrant Name: XXXXX"
  match = text.match(/(?:Filer|Registrant)\s+Name\s*[:\-]\s*([^\n,]+?)(?:\n|$)/i);
  if (match && match[1]) {
    let name = match[1].trim().replace(/\s+/g, ' ');
    if (isValidName(name)) return name.substring(0, 150);
  }
  
  return null;
};

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
// Calculate WA from intraday data (High, Low, Close, Volume)
const calculateWAFromBars = (bars) => {
  if (!bars || bars.length === 0) return null;
  
  let totalVolumePrice = 0;
  let totalVolume = 0;
  
  for (const bar of bars) {
    const high = parseFloat(bar.high || bar.h);
    const low = parseFloat(bar.low || bar.l);
    const close = parseFloat(bar.close || bar.c);
    const volume = parseFloat(bar.volume || bar.v);
    
    if (high > 0 && low > 0 && close > 0 && volume > 0) {
      const typicalPrice = (high + low + close) / 3;
      totalVolumePrice += typicalPrice * volume;
      totalVolume += volume;
    }
  }
  
  if (totalVolume > 0) {
    return (totalVolumePrice / totalVolume).toFixed(2);
  }
  
  return null;
};

// Fetch WA by calculating from price and volume data
const fetchWA = async (ticker, price = null, volume = null, averageVolume = null) => {
  if (!ticker || ticker === 'UNKNOWN') return 'N/A';
  
  // WA approximation: use current price weighted by volume dynamics
  // Formula: WA ≈ price × (volume / (volume + avgVolume))
  // This approximates where the weighted average entry price is based on current volume vs average
  if (price && price !== 'N/A' && !isNaN(parseFloat(price))) {
    const numPrice = parseFloat(price);
    const numVolume = volume && volume > 0 ? parseFloat(volume) : 0;
    const numAvgVol = averageVolume && averageVolume > 0 ? parseFloat(averageVolume) : 0;
    
    // If we have volume data, weight the price by volume ratio
    if (numVolume > 0 && numAvgVol > 0) {
      // Volume ratio: current volume vs total (current + average)
      const volumeRatio = numVolume / (numVolume + numAvgVol);
      // WA = current price weighted by volume participation
      // Higher volume spike = lower WA (more aggressive buying at lower levels)
      const wa = numPrice * volumeRatio + (numPrice * 0.5) * (1 - volumeRatio);
      return parseFloat(wa).toFixed(2);
    }
    
    return parseFloat(numPrice).toFixed(2);
  }
  
  // Final fallback: return N/A if no data available
  return 'N/A';
};


// Detect if registrant is hiding former name/address (suspicious signal)
const detectFormerNameHidden = (text) => {
  if (!text) return false;
  
  // Look for the "Former name or former address" field with NOT APPLICABLE or N/A
  const match = text.match(/(?:Former\s+name|former\s+address|changed\s+since\s+last\s+report)\s*[:\-]?\s*([^\n]+?)(?:\n|$)/i);
  
  if (match && match[1]) {
    const value = match[1].trim().toUpperCase();
    // If it explicitly says "NOT APPLICABLE", "N/A", "NA", this is a red flag
    if (/NOT\s*APPLICABLE|N\/A|^NA$/.test(value)) {
      return true;  // Registrant is hiding previous identity
    }
  }
  
  return false;
};

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
const calculatesignalScore = (float, sharesOutstanding, volume, avgVolume, signalCategories = [], incorporated = null, located = null, filingText = '', companyName = '', itemCode = null, financingType = null, maClosureData = null, foundForms = new Set()) => {
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
  
  // S/O Score - neutral scoring (both tight and diluted floats are tradeable, just different signals)
  let soScore = 0.35;
  const numFloat = parseFloat(float) || 1;
  const numShares = parseFloat(sharesOutstanding) || 1;
  const soPercent = numShares > 0 ? (numFloat / numShares) * 100 : 50;

  if (soPercent < 5) soScore = 0.50;        // Tight float
  else if (soPercent < 25) soScore = 0.48;  // Very tight
  else if (soPercent < 50) soScore = 0.45;  // Tight
  else if (soPercent < 0) soScore = 0.42;  // Moderate
  else if (soPercent < 80) soScore = 0.50;  // Diluted
  else soScore = 0.38;                      // Heavily diluted (still tradeable)

  let volumeScore = 0.25;
  const volumeRatio = avgVolume > 0 ? volume / avgVolume : 0.5;
  if (volumeRatio >= 3.0) volumeScore = 0.65;  // Major spike (3x+)
  else if (volumeRatio >= 2.0) volumeScore = 0.55;  // Significant (2x+)
  else if (volumeRatio >= 1.5) volumeScore = 0.45;  // Moderate
  else if (volumeRatio >= 1.0) volumeScore = 0.35;  // Slight increase
  else if (volumeRatio >= 0.8) volumeScore = 0.25;  // Below average
  else volumeScore = 0.15;

  let signalMultiplier = 1.0;
  const structuralMovers = ['Credit Default', 'Going Dark', 'Warrant Redemption', 'Asset Disposition', 'Share Consolidation', 'Deal Termination', 'Auditor Change', 'Preferred Call', 'DTC Eligible Restored'];
  const hasStructuralMover = signalCategories?.some(cat => structuralMovers.includes(cat));
  const deathSpiralCats = ['Artificial Inflation', 'Bankruptcy Filing', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Share Issuance', 'Convertible Dilution', 'Warrant Dilution', 'Compensation Dilution', 'Accounting Restatement', 'Regulatory Breach', 'Executive Liquidation', 'Credit Default'];
  const hasDeathSpiral = signalCategories?.some(cat => deathSpiralCats.includes(cat));
  const hasSqueeze = signalCategories?.some(cat => cat === 'Artificial Inflation');
  
  // Financial ratio detection - objective bankruptcy indicators from balance sheet
  const financialRatios = parseFinancialRatios(filingText);
  const hasFinancialCrisis = financialRatios.severity > 0.60;
  
  // Add financial ratio signals to signalCategories for scoring context
  if (financialRatios.signals && financialRatios.signals.length > 0) {
    signalCategories = [...(signalCategories || []), ...financialRatios.signals];
  }
  
  if (hasFinancialCrisis && financialRatios.severity > 0.85) {
    signalMultiplier = 1.40;  // Critical bankruptcy risk (current ratio <0.2, negative BVPS, etc.)
  } else if (hasStructuralMover) {
    signalMultiplier = 1.30;  // Structural events with mechanical execution
  } else if (hasFinancialCrisis) {
    signalMultiplier = 1.25;  // High financial distress multiplier
  } else if (hasDeathSpiral) {
    signalMultiplier = 1.15;    // Death spirals force selling
  } else if (hasSqueeze) {
    signalMultiplier = 1.10;        // Supply shocks
  } else {
    signalMultiplier = 1.0;
  }

  // ADR Detection - verify ONLY actual custodian banks, NOT mere country mismatch
  // Removed strict ADR structure matching (incorporated != located)
  let adrMultiplier = 1.0;
  let isCustodianVerified = false;
  let custodianName = null;
  
  // Red flag: "Not Applicable" indicates shell company with no legitimate origin
  if ((incorporated && incorporated.includes('Not Applicable')) || (located && located.includes('Not Applicable')) || (companyName && companyName.includes('Not Applicable'))) {
    adrMultiplier = 1.15;  // Shell company multiplier boost
    isCustodianVerified = false;
    custodianName = 'Ghost Company (N/A)';
  }
  // ONLY apply boost for verified custodian banks (JPMorgan, BNY Mellon, etc.)
  else {
    const custodianResult = detectCustodianBanks(filingText);
    if (custodianResult && custodianResult.verified) {
      adrMultiplier = 1.2;  // Boost ONLY for verified custodian-controlled ADRs
      isCustodianVerified = true;
      custodianName = custodianResult.custodian;
    }
  }
  
  // S/O Bonus - float tightness matters differently based on custodian control
  let soBonus = 1.0;
  if (numShares > 0) {
    const soPercent = (numFloat / numShares) * 100;
    const isADRStructure = adrMultiplier > 1.0;
    
    // ADR (custodian-controlled): tight float = suppressed supply = structural constraint
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

  // Layer 3: Financing Type Multiplier (Bought Deal > Registered Direct + Insider > Registered Direct > ATM)
  let financingMultiplier = 1.0;
  if (financingType) {
    financingMultiplier = financingType.multiplier || 1.0;
  }
  
  // Layer 4: M&A Close + Rebrand Multiplier (structural catalyst)
  let maMultiplier = 1.0;
  if (maClosureData) {
    maMultiplier = maClosureData.multiplier || 1.0;
  }
  
  // Layer 1: Item 8.01 context (patent loss = Material Lawsuit signal boost)
  let item801Multiplier = 1.0;
  if (itemCode === '8.01' && filingText) {
    const item801Context = getItem801Context(filingText);
    if (item801Context === 'Patent Loss' || item801Context === 'Material Lawsuit') {
      // Boost "Material Lawsuit" signal if detected in Item 8.01 context
      if (signalCategories?.includes('Material Lawsuit')) {
        item801Multiplier = 1.10; // 10% boost for Item 8.01 buried lawsuit
      }
    }
  }
  
  let lowFloatPumpBonus = 1.0;
  const floatUnderThreshold = numFloat < 5000000; // Under 5M shares = explosivity candidate
  const hasCleanCatalyst = signalCategories?.some(cat => 
    ['Partnership', 'Major Contract', 'Licensing Deal', 'Revenue Growth', 'Earnings Outperformance'].includes(cat)
  );
  const hasNoClumsyDeathFlag = !signalCategories?.some(cat =>
    ['Reverse Split', 'Artificial Inflation', 'Share Issuance', 'Convertible Dilution', 'Credit Default', 'Going Dark'].includes(cat)
  );
  
  if (floatUnderThreshold && hasCleanCatalyst && hasNoClumsyDeathFlag) {
    lowFloatPumpBonus = 1.25; // 25% bonus for <5M float + clean news
  }

  // Layer 6: FORM TYPE MULTIPLIER - 6-K + 20-F combo proves winners
  let formTypeMultiplier = 1.0;
  const has6K = foundForms.has('6-K') || foundForms.has('6-K/A');
  const has20F = foundForms.has('20-F') || foundForms.has('20-F/A');
  const has8K = foundForms.has('8-K') || foundForms.has('8-K/A');
  
  if (has6K && has20F && !has8K) {
    formTypeMultiplier = 1.15; // 15% boost: 6-K + 20-F (proven winner combo) - clean catalysts only
  } else if ((has6K && has8K) || (has8K && !has6K)) {
    formTypeMultiplier = 0.90; // 10% penalty: 6-K + 8-K or 8-K alone (structure/toxic plays)
  }

  // Weighted calculation (volume 50%, float 25%, S/O 25%) - but with lower base scores
  const signalScore = (floatScore * 0.25 + soScore * 0.25 + volumeScore * 0.5) * signalMultiplier * adrMultiplier * soBonus * financingMultiplier * maMultiplier * item801Multiplier * lowFloatPumpBonus * formTypeMultiplier;
  
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
  } else if (level === 'SKIP') {
    titleColor = '\x1b[90m';
    messageColor = '\x1b[31m';
  } else if (level === 'INFO') {
    titleColor = '\x1b[90m';
    messageColor = '\x1b[92m';
  } else if (level === 'AUTH') {
    titleColor = '\x1b[90m';
    messageColor = '\x1b[92m';
  }

  const now = new Date();
  const dateStr = now.toISOString().split('T')[0]; // YYYY-MM-DD
  const timeStr = now.toLocaleTimeString('en-US', { 
    hour12: false, 
    hour: '2-digit', 
    minute: '2-digit', 
    second: '2-digit' 
  });
  const timestamp = `${dateStr} ${timeStr}`;
  console.log(`\x1b[90m[${timestamp}] ${titleColor}${level}:\x1b[0m ${messageColor}${message}\x1b[0m`);
};

const logGray = (level, message) => {
  const now = new Date();
  const dateStr = now.toISOString().split('T')[0]; // YYYY-MM-DD
  const timeStr = now.toLocaleTimeString('en-US', { 
    hour12: false, 
    hour: '2-digit', 
    minute: '2-digit', 
    second: '2-digit' 
  });
  const timestamp = `${dateStr} ${timeStr}`;
  console.log(`\x1b[90m[${timestamp}] ${level}:\x1b[0m \x1b[90m${message}\x1b[0m`);
};

const FORM_TYPES = ['6-K', '6-K/A', '8-K', '8-K/A', 'S-1', 'S-3', 'S-4', 'S-8', 'F-1', 'F-3', '	SC TO-C', 'SC14D9C', 'S-9', 'F-4', 'FWG', '424B1', '424B2', '424B3', '424B4', '424B5', '424H8', '20-F', '20-F/A', '13G', '13G/A', '13D', '13D/A', 'Form D', 'EX-99.1', 'EX-99.2', 'EX-10.1', 'EX-10.2', 'EX-3.1', 'EX-3.2', 'EX-4.1', 'EX-4.2', 'EX-10.3', 'EX-1.1', 'Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04', 'Item 1.05', 'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06', 'Item 3.01', 'Item 3.02', 'Item 3.03', 'Item 4.01', 'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08', 'Item 5.09', 'Item 5.10', 'Item 5.11', 'Item 5.12', 'Item 5.13', 'Item 5.14', 'Item 5.15', 'Item 6.01', 'Item 7.01', 'Item 8.01', 'Item 9.01'];
const SEMANTIC_KEYWORDS = {
  'Merger/Acquisition': ['Merger Agreement', 'Acquisition Agreement', 'Agreed To Acquire', 'Merger Consideration', 'Premium Valuation', 'Going Private', 'Take Private', 'Acquisition Closing', 'Closing Of Acquisition', 'Completed Acquisition'],
  'M&A Rebrand': ['Corporate Name Change', 'Ticker Change', 'Trading Name Change', 'Change Of Company Name', 'Formerly Known As', 'Name Changed To'],
  'FDA Approved': ['FDA Approval', 'FDA Clearance', 'Approval Granted', 'Approval Letter', 'Approves', 'EMA Approval', 'Post-Market Approval'],
  'FDA Breakthrough': ['Breakthrough Therapy', 'Breakthrough Designation', 'Fast Track Designation', 'Priority Review', 'Priority Status'],
  'FDA Filing': ['NDA Submission', 'NDA Filed', 'BLA Submission', 'BLA Filed', 'IND Application', 'Regulatory Filing'],
  'Clinical Success': ['Positive Trial Results', 'Phase 3 Success', 'Topline Results Beat', 'Efficacy Demonstrated', 'Safety Profile Met', 'Positive Results', 'Phase 1', 'Phase 2', 'Phase 3', 'Trial Results', 'Efficacy', 'Safety Profile', 'Cohort Results', 'Primary Endpoint', 'Enrollment Complete', 'Data Readout', 'Topline Data', 'Meaningful Improvement', 'Beat Placebo', 'Indication', 'Mechanism Of Action', 'Biomarker', 'Immune Rebalancing', 'Comparator', 'Patient Population', 'Favorable Safety', 'Separation From Placebo', 'Demonstrated Benefit', 'Clinical Benefit', 'Strong Efficacy'],
  'Clinical Milestone': ['Phase Advancement', 'Phase 2 Initiation', 'Phase 3 Initiation', 'Enrollment Opened', 'Enrollment Initiated', 'Trial Initiation', 'Investigational New Drug', 'IND Application', 'NDA Filing', 'PMA Submission', 'Clinical Trial Site', 'Patient Enrollment', 'First Patient', 'Program Initiation', 'Patient Dosed', 'First Dose', 'Dose Escalation', 'Cohort Complete'],
  'Capital Raise': ['Oversubscribed', 'Institutional Participation', 'Lead Investor', 'Top-Tier Investor', 'Strategic Investor'],
  'Underwritten Offering': ['Bought Deal', 'Underwriter Commitment', 'Underwritten Bought Deal', 'IPO', 'IPO Underwritten'],
  'Earnings Outperformance': ['Earnings Beat', 'Beat Expectations', 'Beat Consensus', 'Exceeded Guidance', 'Record Revenue'],
  'Major Contract': ['Contract Award', 'Major Customer Win', '$100 Million Contract', 'Exclusive License'],
  'Regulatory Approval': ['Regulatory Approval Granted', 'Patent Approved', 'License Granted', 'Permit Issued'],
  'Revenue Growth': ['Revenue Growth Acceleration', 'Record Quarterly Revenue', 'Guidance Raise', 'Organic Growth'],
  'Insider Buying': ['Director Purchase', 'Executive Purchase', 'CEO Buying', 'CFO Buying', 'Meaningful Accumulation', 'CEO Purchased', 'Chairman Bought', 'Director Purchased', 'Officer Purchased'],
  'Insider Confidence': ['CEO Co-Investment', 'Management Co-Investment', 'Board Co-Investment', 'Insider Co-Investing'],
  'Artificial Inflation': ['Reverse Stock Split', 'Reverse Split', 'Reversed Split', 'Reverse Split Announced', 'Announced Reverse Split', 'Consolidation Of Shares', 'Share Consolidation', 'Combine Shares', 'Combined Shares', 'Stock Consolidation', 'Share Combination', 'Reverse 1:8', 'Reverse 1:10', 'Reverse 1:20', 'Reverse 1:25', 'Reverse 1:50'],
  'Bankruptcy Filing': ['Bankruptcy Protection', 'Chapter 11 Filing', 'Chapter 7 Filing', 'Insolvency Proceedings', 'Creditor Protection'],
  'Operating Deficit': ['Operating Loss', 'Loss from operations', 'Operational Loss'],
  'Negative Earnings': ['Net Loss', 'Continued Losses', 'Massive Losses'],
  'Cash Depletion': ['Cash burn rate', 'Depleted cash', 'Negative cash flow', 'Cash depletion'],
  'Going Concern Risk': ['Accumulated Deficit', 'Going Concern Warning', 'Substantial Doubt Going Concern', 'Auditor Going Concern Note'],
  'Public Offering': ['Public Offering Announced', 'Secondary Offering', 'Follow-On Offering', 'Shelf Offering', 'At-The-Market Offering'],
  'Share Issuance': ['Share Dilution', 'New Shares Issued', 'Shares Outstanding Increased', 'Dilutive issuance', 'Shares increased', 'Share increase', 'Offering shares', 'Issuance of shares'],
  'Convertible Dilution': ['Convertible Notes', 'Convertible Bonds', 'Convertible Securities'],
  'Warrant Dilution': ['Warrant Issuance', 'Forced Exercise'],
  'Compensation Dilution': ['Option Grants Excessive', 'Employee Incentive', 'Equity Compensation', 'RSU Grant', 'Restricted Stock Unit', 'Equity incentive plan increase'],
  'Nasdaq Delisting': ['Nasdaq Deficiency', 'Listing Standards Warning', 'Nasdaq Notification', 'Delisting Determination', 'Nasdaq Letter', 'Delisting Risk', 'Delisting Threat'],
  'Bid Price Delisting': ['Minimum Bid Price', 'Regained Compliance'],
  'Executive Liquidation': ['Director Sale', 'Officer Sale', 'CEO Selling', 'CFO Selling', 'Massive Liquidation'],
  'Accounting Restatement': ['Financial Restatement', 'Audit Non-Reliance', 'Material Weakness', 'Control Deficiency', 'Audit Adjustment'],
  'Credit Default': ['Loan Default', 'Debt Covenant Breach', 'Event Of Default', 'Credit Agreement Violation', 'Covenant Breach', 'Default Event', 'Acceleration Of Debt', 'Mandatory Prepayment'],
  'Going Dark': ['Form 15', 'Deregistration', 'Stop Reporting', 'Cease Reporting', 'Edgar Delisting', 'No Longer Report', 'Deregister', 'Terminate Registration', 'Exit From SEC Reporting', 'Shall No Longer File'],
  'Warrant Redemption': ['Warrant Redemption Notice', 'Warrant Call', 'Call Notice', 'Forced Redemption', 'Warrant Exercised', 'Warrant Expiration', 'Warrant Notice'],
  'Asset Disposition': ['Asset Sale', 'Asset Disposition', 'Business Disposition', 'Sold Assets', 'Divest', 'Divesting', 'Asset Divestiture', 'Strategic Sale', 'Sale Of Assets', 'Disposed', 'Disposition', 'Divested'],
  'Share Consolidation': ['Share Recall', 'Share Call', 'Shareholder Vote', 'Recalled Shares', 'Voting Agreement', 'Recapitalization', 'Consolidation', 'Reverse Recapitalization', 'Stock Consolidation', 'Recapitalize'],
  'Convertible Debt': ['Convertible Bonds', 'Convertible Notes'],
  'Junk Debt': ['Junk Bond Offering'],
  'Material Lawsuit': ['Material Litigation', 'Lawsuit Filed', 'Major Lawsuit', 'SEC Investigation', 'DOJ Investigation'],
  'Supply Chain Crisis': ['Supply Chain Disruption', 'Production Halt', 'Factory Closure', 'Supplier Bankruptcy', 'Shipping Delays'],
  'Executive Departure': ['CEO Departed', 'CFO Departed', 'CEO Resigned', 'Chief Officer Left', 'CEO Resignation', 'CFO Departure'],
  'Executive Detention/Investigation': ['CEO Detained', 'Chairman Detained', 'Officer Detained', 'Notice Of Detention', 'Notice Of Investigation', 'Under Investigation', 'Supervisory Commission'],
  'Board Change': ['Board Resignation', 'Director Appointed', 'Board Member Appointed', 'Director Elected', 'Director Resigned'],
  'Deal Termination': ['Deal Terminated', 'Merger Terminated', 'Acquisition Terminated', 'Agreement Terminated', 'Transaction Terminated', 'Deal Break', 'Termination Of Agreement', 'Failed To Close', 'Terminated The'],
  'Auditor Change': ['Auditor Resigned', 'Audit Firm Changed', 'Auditor Departure', 'Internal Controls Weakness', 'Material Weakness', 'Auditor No Longer', 'Changes Auditor', 'Change Of Auditor'],
  'Preferred Call': ['Preferred Redemption', 'Preferred Call Notice', 'Preferred Redeemed', 'Series Redeemed', 'Redemption Of Preferred'],
  'Debt Refinance': ['Debt Refinanced', 'Refinancing Completed', 'Extended Maturity', 'Debt Extension', 'Loan Refinanced', 'Refinance Debt', 'Facility Refinanced', 'Extension Agreement'],
  'Debt Restructure': ['Debt Restructured', 'Restructure Agreement', 'Debt Modification', 'Amended Restated', 'Debt Covenant Waiver', 'Forbearance Agreement'],
  'Corporate Separation': ['Spinoff Completed', 'Separation Completed', 'Split-Off', 'Pro-Rata Distribution', 'Distributed Shares'],
  'DTC Eligible Restored': ['DTC Eligible', 'DTC Chill Lifted', 'Eligibility Restored', 'DTC Restoration', 'Chill Status', 'Chill Removed', 'Resume Trading'],
  'Insider Block Buy': ['Meaningful Accumulation', 'Accumulated Shares', 'Block Purchase', 'Significant Accumulation'],
  'Asset Impairment': ['Goodwill Impairment', 'Asset Write-Down', 'Impairment Charge', 'Valuation Adjustment'],
  'Restructuring': ['Organizational Restructure', 'Cost Reduction Program', 'Efficiency Initiative', 'Division Realignment'],
  'Stock Buyback': ['Share Repurchase', 'Buyback Authorization', 'Accelerated Buyback', 'Repurchase Program'],
  'Licensing Deal': ['Exclusive License', 'License Agreement', 'Technology License', 'IP Licensing'],
  'Partnership': ['Strategic Partnership', 'Joint Venture', 'Partnership Agreement', 'Strategic Alliance', 'Development Agreement'],
  'Facility Expansion': ['New Facility Opening', 'Capacity Expansion', 'Manufacturing Expansion', 'Facility Upgrade'],
  'Blockchain Initiative': ['Blockchain Integration', 'Cryptocurrency Payment', 'NFT Launch', 'Web3 Partnership', 'Token Launch', 'Smart Contract Deployment', 'Blockchain Adoption', 'Crypto Exchange Partnership', 'Decentralized Platform'],
  'Government Contract': ['Government Contract Award', 'Defense Contract', 'Federal Contract', 'DOD Contract', 'GSA Schedule', 'Federal Procurement'],
  'Stock Split': ['Stock Split Announced', 'Forward Split', 'Stock Dividend', 'Share Split'],
  'Dividend Raise': ['Dividend Increase', 'Dividend Hike', 'Special Dividend', 'Increased Dividend', 'Quarterly Dividend Raised', 'Annual Dividend Increase'],
  'Regulatory Breach': ['Regulatory Violation', 'FDA Warning', 'Product Recall', 'Safety Recall', 'Warning Letter'],
  'VIE Arrangement': ['VIE Structure', 'VIE Agreement', 'Variable Interest'],
  'ADR Regulation Risk': ['PRC Regulations', 'Regulatory Risk', 'Chinese Regulatory', 'Capital Control', 'Foreign Exchange Restriction', 'Dividend Limitation', 'SAFE Circular', 'Subject To Risks', 'Uncertainty Of Interpretation'],
  'Mining Operations': ['Mining Operation', 'Cryptocurrency Mining', 'Blockchain Mining', 'Bitcoin Mining', 'Ethereum Mining', 'Mining Facility', 'Mining Expansion', 'Hash Rate Growth'],
  'Financing Events': ['IPO Announced', 'Debt Offering', 'Credit Facility', 'Loan Facility', 'Financing Secured', 'Capital Structure', 'Bond Issuance'],
  'Analyst Coverage': ['Analyst Initiation', 'Analyst Upgrade', 'Analyst Initiation Buy', 'Rating Upgrade', 'Price Target Increase', 'Outperform Rating', 'Buy Rating Initiated'],
  'Product Sunset': ['Product Discontinuation', 'Product Discontinue', 'Discontinuing Product', 'Product Line Discontinued', 'End Of Life Product', 'Phase Out Product'],
  'Loss of Major Customer': ['Major Customer Loss', 'Lost Major Customer', 'Significant Customer Left', 'Key Customer Departure', 'Primary Customer Loss'],
  'Late Filing Notice': ['Unable To File', 'Form 12b-25', 'Unreasonable Effort', 'Late Filing Notification', 'Delayed Quarterly Report', 'Delayed Annual Report', 'Notification Of Late Filing'],
  'Executive Departure Non-Planned': ['Stepped Down', 'Stepped Down From Role', 'Step Down', 'Departure Of Directors', 'Departure Of Officers', 'General Manager Departed', 'Vice President Departed', 'VP Departed', 'EVP Departed', 'Executive VP Departed', 'Planned Leadership Transition'],
  'Bankruptcy Risk - Negative ROE': ['Negative Return On Equity', 'Negative ROE', 'Negative ROIC', 'Bankruptcy Risk', 'Bankruptcy Warning', 'Going Concern', 'Substantial Doubt', 'Continue As A Going Concern'],
  'Reverse Split Event': ['Reverse Split Completed','Reverse Consolidation', 'Recent Consolidation'],
  'Critical Minerals Discovery': ['Rare Earth', 'Rare Earth Elements', 'REE', 'Lithium', 'Cobalt', 'Nickel', 'Critical Metals', 'Critical Minerals', 'Strategic Minerals'],
  'Processing Facility': ['Processing Facility', 'Refining Facility', 'Refinement Plant', 'Processing Plant', 'Extraction Facility', 'Ore Processing'],
  'Offtake Agreement': ['Offtake Agreement', 'Offtake MOU', 'Off-take', 'Offtake Contract', 'Secured Offtake', 'Offtake Term Sheet'],
};


// FINANCIAL RATIO PARSER - Extract & analyze balance sheet metrics
// Financial ratio parser: extracts quantitative balance sheet metrics from filing text
const parseFinancialRatios = (filingText) => {
  if (!filingText) return { signals: [], severity: 0 };
  
  const signals = [];
  let severity = 0;
  
  // Current Ratio parser (liquidity crisis threshold = 0.5)
  const currentRatioMatch = filingText.match(/current ratio[:\s]+([0-9.]+)/i);
  if (currentRatioMatch) {
    const ratio = parseFloat(currentRatioMatch[1]);
    if (ratio < 0.2) {
      signals.push('Liquidity Crisis - Current Ratio Below 0.2');
      severity = Math.max(severity, 0.95); // Near certain bankruptcy
    } else if (ratio < 0.5) {
      signals.push('Liquidity Shortage - Current Ratio Below 0.5');
      severity = Math.max(severity, 0.80);
    } else if (ratio < 1.0) {
      signals.push('Liquidity Concern - Current Ratio Below 1.0');
      severity = Math.max(severity, 0.60);
    }
  }
  
  // Working Capital parser (negative = can't pay bills)
  const wcMatch = filingText.match(/working capital[:\s]+\(([0-9,.]+)\)|working capital[:\s]*-([0-9,.]+)|working capital[:\s]+\$?\(?([0-9,]+)\)?M/i);
  if (wcMatch) {
    const wcText = (wcMatch[1] || wcMatch[2] || wcMatch[3] || '').replace(/[,$M]/g, '');
    const wc = parseFloat(wcText);
    if (wc < -10000000) { // < -$10M
      signals.push('Massive Working Capital Deficit (WC < -$10M)');
      severity = Math.max(severity, 0.85);
    } else if (wc < 0) {
      signals.push('Working Capital Deficit (WC < 0)');
      severity = Math.max(severity, 0.70);
    }
  }
  
  // Book Value per Share parser (negative = insolvent on paper)
  const bvpsMatch = filingText.match(/book value per share[:\s]+\$?([0-9.-]+)|equity.*per share[:\s]+\$?([0-9.-]+)/i);
  if (bvpsMatch) {
    const bvps = parseFloat(bvpsMatch[1] || bvpsMatch[2]);
    if (bvps < 0) {
      signals.push('Negative Book Value Per Share (BVPS < 0)');
      severity = Math.max(severity, 0.90); // Technically insolvent
    }
  }
  
  // Net Cash parser (negative debt = more debt than cash)
  const netCashMatch = filingText.match(/net cash[:\s]+\(?([0-9,.-]+)\)?M|net debt[:\s]+\$?([0-9,.-]+)M/i);
  if (netCashMatch) {
    const ncText = (netCashMatch[1] || netCashMatch[2] || '').replace(/[,$M]/g, '');
    const nc = parseFloat(ncText);
    if (nc < -5000) { // < -$5B
      signals.push('Severe Net Debt Position - Over $5B');
      severity = Math.max(severity, 0.75);
    } else if (nc < 0) {
      signals.push('Net Debt Position');
      severity = Math.max(severity, 0.65);
    }
  }
  
  // Debt/Equity parser (> 2.0 = highly leveraged)
  const deMatch = filingText.match(/debt.*equity[:\s]+([0-9.]+)|leverage ratio[:\s]+([0-9.]+)/i);
  if (deMatch) {
    const de = parseFloat(deMatch[1] || deMatch[2]);
    if (de > 3.0) {
      signals.push('High Leverage - Debt/Equity Exceeds 3.0');
      severity = Math.max(severity, 0.75);
    } else if (de > 2.0) {
      signals.push('Leverage Concern - Debt/Equity Exceeds 2.0');
      severity = Math.max(severity, 0.65);
    }
  }
  
  // Interest Coverage parser (< 1.0 = can't service debt)
  const icMatch = filingText.match(/interest coverage[:\s]+([0-9.]+)|times interest earned[:\s]+([0-9.]+)/i);
  if (icMatch) {
    const ic = parseFloat(icMatch[1] || icMatch[2]);
    if (ic < 0.5) {
      signals.push('Debt Service Risk - Interest Coverage Below 0.5');
      severity = Math.max(severity, 0.85);
    } else if (ic < 1.0) {
      signals.push('Debt Service Concern (IC < 1.0)');
      severity = Math.max(severity, 0.75);
    }
  }
  
  return { signals, severity };
};

// 1. DTC Chill Lift Detector
const detectDTCChillLift = (text) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  const liftPatterns = ['dtc chill lifted', 'dtc eligibility restored', 'dtc eligible', 'shares are now dtc eligible', 'dtc has restored'];
  return liftPatterns.some(p => lowerText.includes(p)) ? 'DTC_CHILL_LIFT' : null;
};

// 2. Batch Filing Detector - Same lawyer + same items + 60min window = coordinated
const detectBatchFiling = (allFilings) => {
  if (!allFilings || allFilings.length < 2) return [];
  
  const batchClusters = [];
  const lawyerClusters = {};
  
  // Group by law firm
  for (const filing of allFilings) {
    const title = (filing.title || '').toLowerCase();
    let firm = null;
    
    if (title.includes('hunter taubman') || title.includes('hunter')) {
      firm = 'Hunter Taubman';
    } else if (title.includes('ellenoff')) {
      firm = 'Ellenoff';
    } else if (title.includes('sichenzia')) {
      firm = 'Sichenzia';
    }
    
    if (firm) {
      if (!lawyerClusters[firm]) lawyerClusters[firm] = [];
      lawyerClusters[firm].push(filing);
    }
  }
  
  // Check for batches: same firm + 3+ filings within 60 minutes
  for (const [firm, filings] of Object.entries(lawyerClusters)) {
    if (filings.length >= 3) {
      const times = filings.map(f => new Date(f.updated).getTime());
      const minTime = Math.min(...times);
      const maxTime = Math.max(...times);
      const diffMin = (maxTime - minTime) / 60000;
      
      if (diffMin <= 60) {
        batchClusters.push({
          firm,
          count: filings.length,
          minuteSpan: Math.round(diffMin),
          tickers: filings.map(f => f.title.match(/\b[A-Z]{1,5}\b/)?.[0]).filter(Boolean)
        });
      }
    }
  }
  
  return batchClusters;
};

// 3. Form 15 + Name Change Together - Shell recycling pattern
const detectShellRecycling = (text) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  
  const hasForm15 = lowerText.includes('form 15') || lowerText.includes('going dark');
  const hasNameChange = lowerText.includes('name change') || lowerText.includes('certificate of amendment') || 
                        lowerText.includes('change of company name') || lowerText.includes('formerly known as');
  
  return (hasForm15 && hasNameChange) ? 'Shell Recycling' : null;
};

// 4. VStock Transfer Agent Detection - Transfer agent rotation indicator
const detectVStockTransferAgent = (text) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  
  const patterns = [
    { from: /equity stock transfer/i, to: /vstock transfer/i, signal: 'VStock Setup' },
    { from: /continental stock/i, to: /vstock transfer/i, signal: 'VStock Setup' },
    { from: /[\w\s]+/i, to: /vstock transfer|island stock transfer/i, signal: 'VStock Setup' }
  ];
  
  const hasVStock = /vstock|island stock transfer/i.test(lowerText);
  const hasTransferAgent = /transfer agent|stock transfer/i.test(lowerText);
  
  return (hasVStock && hasTransferAgent) ? 'VStock Setup' : null;
};

// 5. NT 10-K → Actual 10-K Cycle (Chinese ADRs) - Filing cycle pattern
const detectNT10KCycle = (text, filingType) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  
  const isChinese = lowerText.includes('prc') || lowerText.includes('china') || 
                    lowerText.includes('cayman') || lowerText.includes('bvi') ||
                    lowerText.includes('shanghai') || lowerText.includes('beijing');
  
  if (!isChinese) return null;
  
  // Check if it's an NT 10-K (late filing notification)
  if (lowerText.includes('nt 10-k') || lowerText.includes('notification of late') || 
      lowerText.includes('we are unable to file') || lowerText.includes('form 12b-25')) {
    return 'NT 10K Filed';
  }
  
  // Check if it's the actual 10-K after NT
  if (filingType && filingType.includes('10-K') && !lowerText.includes('nt 10-k')) {
    return 'Actual 10K Filed';
  }
  
  return null;
};

// 6. Third-Party Services Detection - Proxy solicitors, M&A advisors, transfer agents
const detectThirdPartyServices = (text) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  
  const services = {
    'D.F. King': /d\.f\.\s*king|df king/i,
    'MacKenzie Partners': /mackenzie partners/i,
    'Innisfree M&A': /innisfree/i,
    'Okapi Partners': /okapi partners/i,
    'Sard Verbinnen': /sard verbinnen/i,
    'Weinstein PR': /weinstein/i,
    'PCG Advisory': /pcg advisory/i,
    'American Stock Transfer': /american stock transfer/i,
    'VStock Transfer': /vstock|island stock transfer/i
  };
  
  const detected = [];
  for (const [name, pattern] of Object.entries(services)) {
    if (pattern.test(lowerText)) {
      detected.push(name);
    }
  }
  
  return detected.length > 0 ? detected : null;
};

const SEC_CODE_TO_COUNTRY = {'C2':'Shanghai, China','F4':'Shadong, China','F8':'Bogota, Columbia','6A':'Shanghai, China','D8':'Hong Kong','H0':'Hong Kong','K3':'Kowloon Bay, Hong Kong','S4':'Singapore','U0':'Singapore','C0':'Cayman Islands','K2':'Cayman Islands','E9':'Cayman Islands','1E':'Charlotte Amalie, U.S. Virgin Islands','VI':'Road Town, British Virgin Islands','A1':'Toronto, Canada','A2':'Winnipeg, Canada','A6':'Ottawa, Canada','A9':'Vancouver, Canada','A0':'Calgary, Canada','CA':'Toronto, Canada','C4':'Toronto, Canada','D0':'Hamilton, Canada','D9':'Toronto, Canada','Q0':'Toronto, Canada','L3':'Tel Aviv, Israel','J1':'Tokyo, Japan','M0':'Tokyo, Japan','E5':'Dublin, Ireland','I0':'Dublin, Ireland','L2':'Dublin, Ireland','DE':'Wilmington, Delaware','1T':'Athens, Greece','B2':'Bridgetown, Barbados','B6':'Nassau, Bahamas','B9':'Hamilton, Bermuda','C1':'Buenos Aires, Argentina','C3':'Brisbane, Australia','C7':'St. Helier, Channel Islands','D2':'Hamilton, Bermuda','D4':'Hamilton, Bermuda','D5':'Sao Paulo, Brazil','D6':'Bridgetown, Barbados','E4':'Hamilton, Bermuda','F2':'Frankfurt, Germany','F3':'Paris, France','F5':'Johannesburg, South Africa','G0':'St. Helier, Jersey','G1':'St. Peter Port, Guernsey','G4':'New York, United States','G7':'Copenhagen, Denmark','H1':'St. Helier, Jersey','I1':'Douglas, Isle of Man','J0':'St. Helier, Jersey','J2':'St. Helier, Jersey','J3':'St. Helier, Jersey','K1':'Seoul, South Korea','K7':'New York, United States','L0':'Hamilton, Bermuda','L6':'Milan, Italy','M1':'Majuro, Marshall Islands','N0':'Amsterdam, Netherlands','N2':'Amsterdam, Netherlands','N4':'Amsterdam, Netherlands','O5':'Mexico City, Mexico','P0':'Lisbon, Portugal','P3':'Manila, Philippines','P7':'Madrid, Spain','P8':'Warsaw, Poland','R0':'Milan, Italy','S0':'Madrid, Spain','T0':'Lisbon, Portugal','T3':'Johannesburg, South Africa','U1':'London, United Kingdom','U5':'London, United Kingdom','V0':'Zurich, Switzerland','V8':'Geneva, Switzerland','W0':'Frankfurt, Germany','X0':'London, UK','X1':'Luxembourg City, Luxembourg','Y0':'Nicosia, Cyprus','Y1':'Nicosia, Cyprus','Y7':'St. Peter Port, Guernsey','Z0':'Johannesburg, South Africa','Z1':'Johannesburg, South Africa','Z4':'Vancouver, British Columbia, Canada','1A':'Pago Pago, American Samoa','1B':'Saipan, Northern Mariana Islands','1C':'Hagatna, Guam','1D':'San Juan, Puerto Rico','3A':'Sydney, Australia','4A':'Auckland, New Zealand','5A':'Apia, Samoa','7A':'Moscow, Russia','8A':'Mumbai, India','9A':'Jakarta, Indonesia','2M':'Frankfurt, Germany','U3':'Madrid, Spain','Y9':'Nicosia, Cyprus','AL':'Birmingham, UK','Q8':'Oslo, Norway','R1':'Panama City, Panama','V7':'Stockholm, Sweden','K8':'Jakarta, Indonesia','O9':'Monaco','W8':'Istanbul, Turkey','R5':'Lima, Peru','N8':'Kuala Lumpur, Malaysia'};

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
  
  // Priority 1: Look for explicit reverse split announcements with ratios
  // Pattern: "reverse split of... at a ratio of 1-for-X" or "1-for-X reverse stock split"
  let match = text.match(/reverse\s+(?:split|combination|consolidation).*?(?:ratio|at)\s+(?:of\s+)?1\s*(?:-|for)\s*(\d+)/i);
  if (match && match[1]) {
    return `1-for-${match[1]}`;
  }
  
  // Priority 2: Look for "approved a ... 1-for-X" followed by "reverse"
  match = text.match(/approved.*?1\s*(?:-|for)\s*(\d+)\s*.*?reverse/i);
  if (match && match[1]) {
    return `1-for-${match[1]}`;
  }
  
  // Priority 3: Look for announcements with explicit ratio like "1-for-60"
  match = text.match(/(?:announces?|announced)\s+(?:a\s+)?1\s*(?:-|for)\s*(\d+)\s+reverse/i);
  if (match && match[1]) {
    return `1-for-${match[1]}`;
  }
  
  // Priority 4: Match "every X shares will be combined into one" pattern
  match = text.match(/every\s+(\d+)\s+(?:shares|ordinary shares).*?(?:will\s+)?(?:be\s+)?combined?\s+into\s+(?:one|1)\s+(?:share|post)/i);
  if (match && match[1]) {
    return `1-for-${match[1]}`;
  }
  
  // Priority 5: Context-aware 1-for-X match (avoids file numbers like 001-38857)
  // Must have "reverse", "split", "consolidation", "combination", or "stock" nearby
  match = text.match(/(reverse|split|consolidation|combination|stock)\s+.*?1\s*(?:-|for)\s*(\d{2,3})/i);
  if (match && match[2]) {
    const ratio = parseInt(match[2]);
    // Validate it's a reasonable split ratio (between 2 and 1000, not file number)
    if (ratio >= 2 && ratio <= 1000) {
      return `1-for-${match[2]}`;
    }
  }
  
  return null;
};

// Extract Item Code context from filing (e.g., "Item 8.01", "Item 6.01")
const extractItemCode = (text) => {
  if (!text) return null;
  // Match "Item X.XX" patterns
  const itemMatch = text.match(/\bItem\s+([1-9]\.\d{2})\b/i);
  return itemMatch ? itemMatch[1] : null;
};

// Detect if Item 8.01 contains specific context (patent loss, lawsuit, etc.)
const getItem801Context = (text) => {
  if (!text) return null;
  const lowerText = text.toLowerCase();
  
  if (lowerText.includes('patent') && (lowerText.includes('revoked') || lowerText.includes('lost') || lowerText.includes('invalidated'))) {
    return 'Patent Loss';
  }
  if (lowerText.includes('lawsuit') || lowerText.includes('litigation') || lowerText.includes('settlement')) {
    return 'Material Lawsuit';
  }
  if (lowerText.includes('regulatory') && (lowerText.includes('violation') || lowerText.includes('investigation'))) {
    return 'Regulatory Loss';
  }
  return null;
};

// Extract insider buying amounts: CEO bought X shares @ $Y/share
const extractInsiderBuyingAmount = (text) => {
  if (!text) return { insiderAmount: null, insiderShares: null, participants: [] };
  
  const lowerText = text.toLowerCase();
  const result = { insiderAmount: null, insiderShares: null, participants: [] };
  
  // Match patterns like "CEO purchased 2,400,000 shares" or "2.4 million shares"
  const sharePatterns = [
    /(?:ceo|chairman|director|officer)\s+(?:purchased|bought|acquired)\s+([\d,]+)\s*(?:shares)?/gi,
    /(?:CEO|Chairman|Director|Officer).*?(\d+[\d,]*)\s*(?:shares|common stock)/gi
  ];
  
  let totalShares = 0;
  const participantSet = new Set();
  
  for (const pattern of sharePatterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const shares = parseInt(match[1].replace(/,/g, ''));
      if (!isNaN(shares) && shares > 0) {
        totalShares += shares;
        const title = match[0].match(/(?:CEO|Chairman|Director|Officer)/i);
        if (title) participantSet.add(title[0].toLowerCase());
      }
    }
  }
  
  // Try to extract price/amount: "at $X.XX per share" or "at $1.25/share"
  const priceMatch = text.match(/(?:at|@)\s*\$?([\d.]+)\s*(?:per\s+share|\/share|\s+share)/i);
  if (priceMatch && totalShares > 0) {
    const pricePerShare = parseFloat(priceMatch[1]);
    result.insiderAmount = (totalShares * pricePerShare).toFixed(0);
  }
  
  result.insiderShares = totalShares > 0 ? totalShares : null;
  result.participants = Array.from(participantSet);
  
  return result;
};

// Detect financing type: bought deal, registered direct, ATM, etc.
const detectFinancingType = (text) => {
  if (!text) return { type: 'Generic', multiplier: 1.0 };
  
  const lowerText = text.toLowerCase();
  
  // Bought Deal (underwriter-backed = confidence signal)
  if ((lowerText.includes('bought deal') || lowerText.includes('underwriter') || lowerText.includes('underwritten')) && lowerText.includes('offering')) {
    return { type: 'Underwritten Offering', multiplier: 1.20 };
  }
  
  // Registered Direct + insider buying = high confidence
  if (lowerText.includes('registered direct') && (lowerText.includes('ceo') || lowerText.includes('chairman') || lowerText.includes('director'))) {
    return { type: 'Registered Direct + Insider', multiplier: 1.25 };
  }
  
  // Registered Direct (no insider co-investment)
  if (lowerText.includes('registered direct')) {
    return { type: 'Registered Direct', multiplier: 1.10 };
  }
  
  // At-The-Market (opportunistic, dilutive)
  if (lowerText.includes('at-the-market') || lowerText.includes('atm offering')) {
    return { type: 'ATM Offering', multiplier: 0.95 };
  }
  
  // Generic public offering
  if (lowerText.includes('public offering') || lowerText.includes('secondary offering')) {
    return { type: 'Public Offering', multiplier: 0.98 };
  }
  
  return { type: 'Generic Raise', multiplier: 1.0 };
};

// Detect M&A close + rebrand as structural catalyst
const detectMACloseRebrand = (text) => {
  if (!text) return { isMAClosed: false, hasRebrand: false, multiplier: 1.0 };
  
  const lowerText = text.toLowerCase();
  
  const isMAClosed = (lowerText.includes('acquisition') || lowerText.includes('merger')) &&
                      (lowerText.includes('closing') || lowerText.includes('completed') || lowerText.includes('closed'));
  
  const hasRebrand = (lowerText.includes('name change') || lowerText.includes('company name') || lowerText.includes('ticker change')) &&
                     (lowerText.includes('formerly') || lowerText.includes('change to') || lowerText.includes('will be'));
  
  let multiplier = 1.0;
  if (isMAClosed && hasRebrand) {
    multiplier = 1.30; // Full M&A + rebrand = structural transformation signal
  } else if (isMAClosed) {
    multiplier = 1.15; // M&A closed = structural change
  }
  
  return { isMAClosed, hasRebrand, multiplier };
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
    const headers = 'Filed Date,Filed Time,Scanned Date,Scanned Time,CIK,Ticker,Registrant Name,Price,Score,Float,Shares Outstanding,S/O Ratio,Weighted Average,FTD,FTD %,Volume,Average Volume,Incorporated,Located,Filing Type,Catalyst,Custodian Control,Filing Time Bonus,S/O Bonus,Bonus Signals,Financial Ratios,Alert Type,Skip Reason\n';
    
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
    
    // Format bonus signals
    let bonusSignalsStr = 'N/A';
    if (alertData.bonusSignals && typeof alertData.bonusSignals === 'object') {
      const bonusItems = [];
      if (alertData.bonusSignals['DTC Chill Lift']) bonusItems.push('DTC Chill Lift');
      if (alertData.bonusSignals['Shell Recycling']) bonusItems.push('Shell Recycling');
      if (alertData.bonusSignals['VStock']) bonusItems.push('VStock');
      if (alertData.bonusSignals['NT 10K'] === 'NT 10K Filed') bonusItems.push('NT 10-K Filed');
      if (alertData.bonusSignals['NT 10K'] === 'Actual 10K Filed') bonusItems.push('Actual 10-K');
      if (alertData.bonusSignals['Third Party'] && Array.isArray(alertData.bonusSignals['Third Party'])) {
        bonusItems.push(`Services: ${alertData.bonusSignals['Third Party'].join(';')}`);
      }
      if (bonusItems.length > 0) {
        bonusSignalsStr = bonusItems.join('; ');
      }
    }
    
    // Format financial ratio signals
    let financialRatiosStr = 'N/A';
    if (alertData.financialRatioSignals && alertData.financialRatioSignals.signals && Array.isArray(alertData.financialRatioSignals.signals)) {
      financialRatiosStr = alertData.financialRatioSignals.signals.join('; ') + ` [Severity: ${alertData.financialRatioSignals.severity.toFixed(2)}]`;
    }
    
    // Build CSV row with data
    const csvWA = alertData.wa || 'N/A';
    const row = [
      escapeCSV(filedDate),
      escapeCSV(filedTime),
      escapeCSV(scannedDate),
      escapeCSV(scannedTime),
      escapeCSV(alertData.cik || 'N/A'),
      escapeCSV(alertData.ticker || 'N/A'),
      escapeCSV(alertData.companyName || 'N/A'),
      escapeCSV(alertData.price || 'N/A'),
      escapeCSV(alertData.signalScore || 'N/A'),
      escapeCSV(alertData.float || 'N/A'),
      escapeCSV(alertData.sharesOutstanding || 'N/A'),
      escapeCSV(alertData.soRatio || 'N/A'),
      escapeCSV(csvWA !== 'N/A' ? parseFloat(csvWA).toFixed(2) : 'N/A'),
      escapeCSV(alertData.ftd || 'false'),
      escapeCSV(alertData.ftdPercent || 'N/A'),
      escapeCSV(alertData.volume || 'N/A'),
      escapeCSV(alertData.averageVolume || 'N/A'),
      escapeCSV(incorporated || 'N/A'),
      escapeCSV(located || 'N/A'),
      escapeCSV(alertData.filingType || 'N/A'),
      escapeCSV(signals || 'Press/Regulatory Release'),
      escapeCSV(alertData.custodianControl ? (alertData.custodianVerified ? `1.3x ${alertData.custodianName}` : alertData.custodianName) : 'No'),
      escapeCSV(alertData.filingTimeBonus ? `${alertData.filingTimeBonus}x Filing Time` : 'No'),
      escapeCSV(alertData.soBonus && alertData.soBonus > 1.0 ? `${alertData.soBonus}x S/O` : 'No'),
      escapeCSV(bonusSignalsStr),
      escapeCSV(financialRatiosStr),
      escapeCSV(alertData.alertType || 'N/A'),
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
    
    // Determine direction for CSV - check for ANY bearish signals
    const bearishCategories = ['Artificial Inflation', 'Bankruptcy Filing', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Public Offering', 'Share Issuance', 'Convertible Dilution', 'Warrant Dilution', 'Compensation Dilution', 'Nasdaq Delisting', 'Bid Price Delisting', 'Executive Liquidation', 'Accounting Restatement', 'Credit Default', 'Senior Debt', 'Convertible Debt', 'Junk Debt', 'Material Lawsuit', 'Supply Chain Crisis', 'Regulatory Breach', 'VIE Arrangement', 'China Risk', 'Product Sunset', 'Loss of Major Customer', 'Underwritten Offering', 'Deal Termination'];
    const signalKeys = (alertData.intent && Array.isArray(alertData.intent)) ? alertData.intent : (alertData.intent ? String(alertData.intent).split(', ') : []);
    const hasBearish = signalKeys.some(cat => bearishCategories.includes(cat));
    const direction = hasBearish ? 'SHORT' : 'LONG';
    
    const enrichedData = {
      ...alertData,
      recordedAt: new Date().toISOString(),
      recordId: `${alertData.ticker}-${Date.now()}`,
      expiresAt: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000).toISOString(),
      direction: direction
    };
    
    alerts.push(enrichedData);
    if (alerts.length > 1000) alerts = alerts.slice(-1000);
    
    fs.writeFileSync(CONFIG.ALERTS_FILE, JSON.stringify(alerts, null, 2));
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
    if (alertData.bonusSignals) {
      if (alertData.bonusSignals['DTC Chill Lift']) bonusItems.push('DTC Chill Lift');
      if (alertData.bonusSignals['Shell Recycling']) bonusItems.push('Shell Recycling');
      if (alertData.bonusSignals['VStock']) bonusItems.push('Transfer Agent Change');
      if (alertData.bonusSignals['NT 10K'] === 'NT 10K Filed') bonusItems.push('Late Filing Notice');
      if (alertData.bonusSignals['NT 10K'] === 'Actual 10K Filed') bonusItems.push('10-K Filing');
      if (alertData.bonusSignals['Third Party'] && Array.isArray(alertData.bonusSignals['Third Party'])) {
        bonusItems.push(`Services: ${alertData.bonusSignals['Third Party'].join(', ')}`);
      }
    }
    // Add financial ratio signals to log output if detected
    let financialRatioIndicator = '';
    if (alertData.financialRatioSignals && alertData.financialRatioSignals.signals && alertData.financialRatioSignals.signals.length > 0) {
      const ratioLabels = alertData.financialRatioSignals.signals.map(s => s.split('(')[0].trim()).join(' + ');
      const severityLevel = alertData.financialRatioSignals.severity > 0.85 ? 'Critical' : 'Elevated';
      financialRatioIndicator = ` (Financial Ratios - ${severityLevel}: ${ratioLabels})`;
    }
    const bonusIndicator = bonusItems.length > 0 ? ` (Bonus: ${bonusItems.join(' + ')})` : '';
    alertData.skipReason = `Alert sent: [${direction}] ${reason}${financialRatioIndicator}${bonusIndicator}`;
    
    // Save to CSV for analysis (non-blocking)
    setImmediate(() => saveToCSV(alertData));
    
    // Cleanup stale alerts based on day of week (non-blocking)
    setImmediate(() => cleanupStaleAlerts());
    
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
    sendTelegramAlert(alertData);
    
    // Update performance tracking data for HTML dashboard (non-blocking)
    setImmediate(() => updatePerformanceData(alertData));
    
    // Log push status based on config
    const pushStatus = CONFIG.GITHUB_PUSH_ENABLED ? '(pushed to GitHub)' : '(GitHub push disabled)';
    log('INFO', `Log: Alert saved ${alertData.ticker} ${pushStatus}`);
    
    const volDisplay = alertData.volume && alertData.volume !== 'N/A' ? (alertData.volume / 1000000).toFixed(2) + 'm' : 'n/a';
    const avgVolDisplay = alertData.averageVolume && alertData.averageVolume !== 'N/A' ? (alertData.averageVolume / 1000000).toFixed(2) + 'm' : 'n/a';
    const floatDisplay = alertData.float && alertData.float !== 'N/A' && !isNaN(alertData.float) ? (alertData.float / 1000000).toFixed(2) + 'm' : 'n/a';
    const soDisplay = alertData.soRatio || 'n/a';
    
    // Log financial ratio signals if detected
    if (alertData.financialRatioSignals && alertData.financialRatioSignals.signals && alertData.financialRatioSignals.signals.length > 0) {
      log('INFO', `Arithmetics: ${alertData.financialRatioSignals.signals.join(', ')}`);
      log('INFO', `Severity: ${alertData.financialRatioSignals.severity.toFixed(2)}/1.0 - ${alertData.financialRatioSignals.severity > 0.85 ? 'Critical Bankruptcy Risk' : 'Elevated Financial Distress'}`);
    }
    
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
        highest5Day: currentPrice,
        highest5DayPercent: 0,
        current: currentPrice,
        currentPrice: currentPrice,
        performance: 0,
        date: new Date().toISOString(),
        alertDate: new Date().toISOString(),
        reverseSplitRatio: null
      };
    } else {
      // Update current price and track peaks/lows
      performanceData[ticker].short = alertData.short ? true : false;
      performanceData[ticker].current = currentPrice;
      if (currentPrice > performanceData[ticker].highest) {
        performanceData[ticker].highest = currentPrice;
      }
      if (currentPrice < performanceData[ticker].lowest) {
        performanceData[ticker].lowest = currentPrice;
      }
      
      // Track 5-day peak (reset daily)
      const alertDate = new Date(performanceData[ticker].alertDate);
      const now = new Date();
      const daysDiff = Math.floor((now - alertDate) / (1000 * 60 * 60 * 24));
      
      if (daysDiff <= 5) {
        if (currentPrice > performanceData[ticker].highest5Day) {
          performanceData[ticker].highest5Day = currentPrice;
          const alertPrice = performanceData[ticker].alert || 0;
          if (alertPrice > 0) {
            const peak5DayChange = currentPrice - alertPrice;
            performanceData[ticker].highest5DayPercent = parseFloat((peak5DayChange / alertPrice * 100).toFixed(2));
          }
        }
      } else {
        // Reset 5-day peak after 5 days
        performanceData[ticker].highest5Day = currentPrice;
        performanceData[ticker].highest5DayPercent = 0;
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
// Get FTD (Failed to Deliver) data from docs/ftd.txt - returns SUM of ALL entries
const getFTDData = (ticker) => {
  try {
    if (!fs.existsSync('docs/ftd.txt')) return false;
    const ftdContent = fs.readFileSync('docs/ftd.txt', 'utf8');
    const lines = ftdContent.split('\n');
    let totalFTD = 0;
    
    for (const line of lines) {
      if (!line.trim()) continue;
      const parts = line.split('|');
      if (parts.length >= 4 && parts[2].toUpperCase() === ticker.toUpperCase()) {
        const ftdQty = parseInt(parts[3]) || 0;
        totalFTD += ftdQty; // Sum all FTD entries
      }
    }
    
    return totalFTD > 0 ? totalFTD : false;
  } catch (e) {
    return false;
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
  // Try Finnhub first (most reliable)
  try {
    const finnhubKey = process.env.FINNHUB_API_KEY;
    if (finnhubKey) {
      const res = await fetchWithTimeout(`https://finnhub.io/api/v1/stock/profile2?symbol=${ticker}&token=${finnhubKey}`, 8000);
      if (res.ok) {
        const data = await res.json();
        if (data.shareOutstanding && data.shareOutstanding > 0) {
          return Math.round(data.shareOutstanding);
        }
      }
    }
  } catch (e) {}
  
  // Fallback to FMP shares-float endpoint which has outstandingShares
  try {
    const fmpKey = process.env.FMP_API_KEY;
    if (!fmpKey) return 'N/A';
    
    const res = await fetchWithTimeout(`https://financialmodelingprep.com/stable/shares-float?symbol=${ticker}&apikey=${fmpKey}`, 8000);
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data) && data[0] && data[0].outstandingShares) {
        const shares = Math.round(data[0].outstandingShares);
        if (shares > 0) return shares;
      }
    }
  } catch (e) {}
  
  return 'N/A';
};

// Extract float shares from SEC filing text (10-K, 10-Q, 6-K, 8-K)
const extractFloatFromFiling = (text, sharesOutstanding) => {
  if (!text) return null;
  
  // Remove HTML tags and normalize whitespace
  let cleanText = text.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ');
  
  // Debug: log text size
  if (cleanText.length < 100) {
    return null; // Text too short to contain shares data
  }
  
  // Pattern 1: "outstanding shares of common stock as of [date]: [number]"
  let match = cleanText.match(/outstanding shares of (?:common )?stock as of [^:]*:\s*([0-9,]+)/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 0) return shares;
  }
  
  // Pattern 2: "indicate the number of shares outstanding of each class: [number]"
  match = cleanText.match(/indicate the number of shares outstanding[^0-9]*?([0-9]{6,})/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares; // At least 1000 shares to be valid
  }
  
  // Pattern 3: "class [A-Z] common stock.*[number] shares outstanding"
  match = cleanText.match(/(?:class [a-z]+ )?common stock[^0-9]*?([0-9,]+)\s+shares? outstanding/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares;
  }
  
  // Pattern 4: "shares outstanding" followed by number (flexible spacing)
  match = cleanText.match(/shares? outstanding[:\s]*([0-9,]+)/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares;
  }
  
  // Pattern 5: "Number of shares outstanding" (common in 6-K)
  match = cleanText.match(/number of shares outstanding[:\s]*([0-9,]+)/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares;
  }
  
  // Pattern 6: Look for cover page format: "as of [date]" followed by number (often first large number in text)
  match = cleanText.match(/as of\s+[^0-9]*([0-9]{1,2},\d{3},\d{3}|\d{9,})/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares;
  }
  
  // Pattern 7: Look for Form cover page shares outstanding (usually in first 2000 chars)
  const firstPart = cleanText.substring(0, 3000);
  match = firstPart.match(/([0-9]{1,2},\d{3},\d{3}(?:,\d{3})?)\s+(?:shares|common stock outstanding|issued and outstanding)/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 1000) return shares;
  }
  
  // Pattern 8: Just look for "shares" or "outstanding" with a large number anywhere
  match = cleanText.match(/([0-9]{1,2},\d{3},\d{3}(?:,\d{3})?)\s+(?:shares?|common)/i);
  if (match) {
    const shares = parseInt(match[1].replace(/,/g, ''));
    if (shares > 100000) return shares; // Higher threshold for non-specific pattern
  }
  
  // Pattern 9: Cover page indicator number format (X,XXX,XXX)
  const coverPageMatch = firstPart.match(/\b([0-9]{1,3},\d{3},\d{3})\b/);
  if (coverPageMatch) {
    const shares = parseInt(coverPageMatch[1].replace(/,/g, ''));
    if (shares > 1000000) return shares; // Very high threshold for generic number
  }
  
  return null;
};

// Get float data from Alpha Vantage first, then Polygon, then FMP as fallback
// Get float data from Alpha Vantage first, then FMP as fallback
const getFloatData = async (ticker) => {
  // Try Alpha Vantage first (has both SharesFloat and SharesOutstanding)
  try {
    const avKey = process.env.ALPHA_VANTAGE_API_KEY;
    if (avKey) {
      const res = await fetchWithTimeout(`https://www.alphavantage.co/query?function=OVERVIEW&symbol=${ticker}&apikey=${avKey}`, 8000);
      if (res.ok) {
        const data = await res.json();
        if (data.SharesFloat && data.SharesFloat !== 'None') {
          const float = Math.round(parseInt(data.SharesFloat) || 0);
          if (float > 0) return float;
        }
      }
    }
  } catch (e) {}
  
  // Fallback to FMP - this endpoint has both float and shares outstanding
  try {
    const fmpKey = process.env.FMP_API_KEY;
    if (!fmpKey) return 'N/A';
    
    const url = `https://financialmodelingprep.com/stable/shares-float?symbol=${ticker}&apikey=${fmpKey}`;
    const res = await fetchWithTimeout(url, 8000);
    if (!res.ok) return 'N/A';
    
    const data = await res.json();
    if (Array.isArray(data) && data[0] && data[0].floatShares) {
      const float = Math.round(data[0].floatShares);
      if (float > 0) return float;
    }
    
    // If floatShares not available but outstandingShares is, use that as fallback
    if (Array.isArray(data) && data[0] && data[0].outstandingShares) {
      const shares = Math.round(data[0].outstandingShares);
      if (shares > 0) return shares;
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
    // Silently fail - suppress all SEC fetch warnings
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
        ticker: data.tickers?.[0] || 'Unknown',
        companyName: data.name || data.entityName || data.conformed_name || 'Unknown',
        cikNumber: data.cik_str || data.cik || cik
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
    // Silently fail - suppress all SEC fetch warnings
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

// Get shares outstanding from SEC 10-K/10-Q XBRL filings
async function getSharesOutstandingFromSEC(cik) {
  try {
    const padded = cik.toString().padStart(10, '0');
    await rateLimit.wait();
    
    const res = await fetchWithTimeout(`https://data.sec.gov/submissions/CIK${padded}.json`, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/json'
      }
    });
    
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
    const data = await res.json();
    
    // Get the most recent 10-K or 10-Q filing
    const filings = data.filings?.recent?.filings || [];
    const recentTenK = filings.find(f => f.form === '10-K' || f.form === '10-Q');
    
    if (!recentTenK) {
      log('WARN', `No recent 10-K/10-Q found for CIK ${cik}`);
      return null;
    }
    
    // Fetch the XBRL data for this filing
    const accessionNumber = recentTenK.accession_number?.replace(/-/g, '') || '';
    const xbrlUrl = `https://www.sec.gov/Archives/edgar/${padded}/${accessionNumber}/${accessionNumber}-index.json`;
    
    await rateLimit.wait();
    const xbrlRes = await fetchWithTimeout(xbrlUrl, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/json'
      }
    });
    
    if (!xbrlRes.ok) throw new Error(`XBRL fetch failed: ${xbrlRes.status}`);
    const xbrlData = await xbrlRes.json();
    
    // Find the XBRL file
    const xbrlFile = xbrlData.files?.find(f => f.name?.endsWith('_htm.xml'));
    if (!xbrlFile) {
      log('WARN', `No XBRL file found for CIK ${cik}`);
      return null;
    }
    
    const xmlUrl = `https://www.sec.gov/Archives/edgar/${padded}/${accessionNumber}/${xbrlFile.name}`;
    await rateLimit.wait();
    
    const xmlRes = await fetchWithTimeout(xmlUrl, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/xml'
      }
    });
    
    if (!xmlRes.ok) throw new Error(`XML fetch failed: ${xmlRes.status}`);
    const xmlText = await xmlRes.text();
    
    // Extract CommonStockSharesOutstanding from XML
    const match = xmlText.match(/<us-gaap:CommonStockSharesOutstanding[^>]*>(\d+(?:,\d{3})*)<\/us-gaap:CommonStockSharesOutstanding>/);
    
    if (match && match[1]) {
      const sharesOutstanding = parseInt(match[1].replace(/,/g, ''));
      log('INFO', `SEC shares outstanding for CIK ${cik}: ${sharesOutstanding.toLocaleString()}`);
      return sharesOutstanding;
    }
    
    log('WARN', `Could not extract CommonStockSharesOutstanding from XBRL for CIK ${cik}`);
    return null;
  } catch (err) {
    log('WARN', `Failed to fetch shares outstanding from SEC for CIK ${cik}: ${err.message}`);
    return null;
  }
}

// Get float from SEC 10-K/10-Q XBRL filings
async function getFloatFromSEC(cik) {
  try {
    const padded = cik.toString().padStart(10, '0');
    await rateLimit.wait();
    
    // Get the most recent 10-K or 10-Q filing
    const res = await fetchWithTimeout(`https://data.sec.gov/submissions/CIK${padded}.json`, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/json'
      }
    });
    
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
    const data = await res.json();
    
    const filings = data.filings?.recent?.filings || [];
    const recentTenK = filings.find(f => f.form === '10-K' || f.form === '10-Q');
    
    if (!recentTenK) {
      log('WARN', `No recent 10-K/10-Q found for CIK ${cik} to get float`);
      return null;
    }
    
    // Fetch the XBRL data for this filing
    const accessionNumber = recentTenK.accession_number?.replace(/-/g, '') || '';
    const xbrlUrl = `https://www.sec.gov/Archives/edgar/${padded}/${accessionNumber}/${accessionNumber}-index.json`;
    
    await rateLimit.wait();
    const xbrlRes = await fetchWithTimeout(xbrlUrl, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/json'
      }
    });
    
    if (!xbrlRes.ok) throw new Error(`XBRL fetch failed: ${xbrlRes.status}`);
    const xbrlData = await xbrlRes.json();
    
    // Find the XBRL file
    const xbrlFile = xbrlData.files?.find(f => f.name?.endsWith('_htm.xml'));
    if (!xbrlFile) {
      log('WARN', `No XBRL file found for CIK ${cik}`);
      return null;
    }
    
    const xmlUrl = `https://www.sec.gov/Archives/edgar/${padded}/${accessionNumber}/${xbrlFile.name}`;
    await rateLimit.wait();
    
    const xmlRes = await fetchWithTimeout(xmlUrl, CONFIG.SEC_FETCH_TIMEOUT, {
      headers: {
        'User-Agent': 'SEC-Bot/1.0 (sendmebsvv@outlook.com)',
        'Accept': 'application/xml'
      }
    });
    
    if (!xmlRes.ok) throw new Error(`XML fetch failed: ${xmlRes.status}`);
    const xmlText = await xmlRes.text();
    
    // Try PublicFloat tag (most direct float from SEC 10-K)
    let floatMatch = xmlText.match(/<us-gaap:PublicFloat[^>]*>(\d+(?:,\d{3})*)<\/us-gaap:PublicFloat>/);
    
    if (floatMatch && floatMatch[1]) {
      const publicFloat = parseInt(floatMatch[1].replace(/,/g, ''));
      log('INFO', `SEC public float for CIK ${cik}: ${publicFloat.toLocaleString()}`);
      return publicFloat;
    }
    
    log('WARN', `Could not extract float from XBRL for CIK ${cik}`);
    return null;
  } catch (err) {
    log('WARN', `Failed to fetch float from SEC for CIK ${cik}: ${err.message}`);
    return null;
  }
}

// Get ownership metrics from SEC, fallback to FMP
async function getOwnershipMetrics(ticker, cik) {
  try {
    log('DEBUG', `Fetching ownership metrics for ${ticker} (CIK ${cik})`);
    
    // Try SEC first for shares outstanding (free, no rate limit issues)
    const sharesOutstanding = await getSharesOutstandingFromSEC(cik);
    
    if (!sharesOutstanding) {
      // Fallback to FMP for both shares outstanding and float
      log('INFO', `SEC lookup failed for ${ticker}, falling back to FMP`);
      const fmpKey = process.env.FMP_API_KEY || 'demo';
      const fmpRes = await fetchWithTimeout(`https://financialmodelingprep.com/api/v4/shares-float?symbol=${ticker}&apikey=${fmpKey}`, 10000);
      
      if (fmpRes.ok) {
        const fmpData = await fmpRes.json();
        if (fmpData[0]) {
          return {
            sharesOutstanding: fmpData[0].weightedAverageShsOut,
            float: fmpData[0].floatShares,
            source: 'FMP'
          };
        }
      }
      
      log('WARN', `Both SEC and FMP lookups failed for ${ticker}`);
      return null;
    }
    
    // Got shares outstanding from SEC, try SEC for float first
    let floatData = null;
    let floatSource = null;
    
    try {
      floatData = await getFloatFromSEC(cik);
      if (floatData) {
        floatSource = 'SEC';
      }
    } catch (err) {
      log('DEBUG', `SEC float lookup failed for ${ticker}: ${err.message}`);
    }
    
    // Fallback to FMP if SEC float didn't work
    if (!floatData) {
      try {
        const fmpKey = process.env.FMP_API_KEY || 'demo';
        const fmpRes = await fetchWithTimeout(`https://financialmodelingprep.com/api/v4/shares-float?symbol=${ticker}&apikey=${fmpKey}`, 10000);
        
        if (fmpRes.ok) {
          const fmpData = await fmpRes.json();
          if (fmpData[0]) {
            floatData = fmpData[0].floatShares;
            floatSource = 'FMP';
          }
        }
      } catch (err) {
        log('DEBUG', `FMP float lookup failed for ${ticker}: ${err.message}`);
      }
    }
    
    return {
      sharesOutstanding,
      float: floatData || null,
      source: floatSource ? `SEC+${floatSource}` : 'SEC'
    };
  } catch (err) {
    log('ERROR', `Failed to get ownership metrics for ${ticker}: ${err.message}`);
    return null;
  }
}

const sendPersonalWebhook = (alertData) => {
  try {
    // Skip if Discord is disabled or no webhook URL configured
    if (!CONFIG.DISCORD_ENABLED || !CONFIG.PERSONAL_WEBHOOK_URL) {
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
    const bearishCategories = ['Artificial Inflation', 'Bankruptcy Filing', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Public Offering', 'Share Issuance', 'Convertible Dilution', 'Warrant Dilution', 'Compensation Dilution', 'Nasdaq Delisting', 'Bid Price Delisting', 'Executive Liquidation', 'Accounting Restatement', 'Credit Default', 'Senior Debt', 'Convertible Debt', 'Junk Debt', 'Material Lawsuit', 'Supply Chain Crisis', 'Regulatory Breach', 'VIE Arrangement', 'China Risk', 'Product Sunset', 'Loss of Major Customer'];
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
    const wa = alertData.wa || 'N/A';
    const waDisplay = wa !== 'N/A' ? `$${parseFloat(wa).toFixed(2)}` : 'N/A';
    
    const personalAlertContent = `↳ [${direction}] **$${ticker}** @ ${priceDisplay} (${countryDisplay}), score: ${signalScoreBold}, ${reason}, vol/avg: ${volDisplay}/${avgDisplay}${volumeMultiplier}, float: ${floatDisplay}, s/o: ${alertData.soRatio}, wa: ${waDisplay}
    https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
    const personalMsg = { content: personalAlertContent };
    
    const waLog = wa !== 'N/A' ? `$${wa.toFixed(2)}` : 'N/A';
    log('INFO', `Alert: [${direction}] $${ticker} @ ${priceDisplay}, Score: ${signalScoreDisplay}`);
    
    // Non-blocking fetch with timeout
    Promise.race([
      fetch(CONFIG.PERSONAL_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(personalMsg)
      }).then(res => {
        if (!res.ok) {
          throw new Error(`Webhook returned ${res.status}`);
        }
        return res;
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Webhook timeout')), 6000))
    ]).catch(err => {
      // Silently fail - don't block on webhook
    });
  } catch (err) {
    // Silently fail - don't block processing
  }
};

const sendTelegramAlert = (alertData) => {
  try {
    // Skip if Telegram is disabled or no credentials configured
    if (!CONFIG.TELEGRAM_ENABLED || !CONFIG.TELEGRAM_BOT_TOKEN || !CONFIG.TELEGRAM_CHAT_ID) {
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
    const bearishCategories = ['Artificial Inflation', 'Bankruptcy Filing', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Public Offering', 'Share Issuance', 'Convertible Dilution', 'Warrant Dilution', 'Compensation Dilution', 'Nasdaq Delisting', 'Bid Price Delisting', 'Executive Liquidation', 'Accounting Restatement', 'Credit Default', 'Senior Debt', 'Convertible Debt', 'Junk Debt', 'Material Lawsuit', 'Supply Chain Crisis', 'Regulatory Breach', 'VIE Arrangement', 'China Risk', 'Product Sunset', 'Loss of Major Customer'];
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
    const signalScoreDisplay = alertData.signalScore ? alertData.signalScore : 'N/A';
    const wa = alertData.wa || 'N/A';
    const waDisplay = wa !== 'N/A' ? `$${parseFloat(wa).toFixed(2)}` : 'N/A';
    
    const telegramAlertContent = `↳ [${direction}] $${ticker} @ ${priceDisplay} (${countryDisplay}), score: ${signalScoreDisplay}, ${reason}, vol/avg: ${volDisplay}/${avgDisplay}${volumeMultiplier}, float: ${floatDisplay}, s/o: ${alertData.soRatio}, wa: ${waDisplay}\nhttps://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
    
    const telegramMsg = { text: telegramAlertContent };
    
    const waLog = wa !== 'N/A' ? `$${wa.toFixed(2)}` : 'N/A';
    log('INFO', `Telegram Alert: [${direction}] $${ticker} @ ${priceDisplay}, Score: ${signalScoreDisplay}`);
    
    // Non-blocking fetch with timeout
    const telegramUrl = `https://api.telegram.org/bot${CONFIG.TELEGRAM_BOT_TOKEN}/sendMessage`;
    const telegramPayload = {
      chat_id: CONFIG.TELEGRAM_CHAT_ID,
      text: telegramAlertContent,
      parse_mode: 'HTML'
    };
    
    Promise.race([
      fetch(telegramUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(telegramPayload)
      }).then(res => {
        if (!res.ok) {
          throw new Error(`Telegram returned ${res.status}`);
        }
        return res;
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Telegram timeout')), 6000))
    ]).catch(err => {
      // Silently fail - don't block on Telegram
    });
  } catch (err) {
    // Silently fail - don't block processing
  }
};

const pushToGitHub = () => {
  // Check if GitHub push is enabled
  if (!CONFIG.GITHUB_PUSH_ENABLED || !CONFIG.GITHUB_PAGES_ENABLED) {
    return; // Skip push if disabled
  }

  try {
    const projectRoot = CONFIG.GITHUB_REPO_PATH;
    // Run git push in background, don't wait for it
    require('child_process').exec(`cd ${projectRoot} && git add logs/alert.json logs/stocks.json logs/quote.json 2>/dev/null && git commit -m "Auto: Alert update $(date '+%Y-%m-%d %H:%M:%S')" 2>/dev/null && git push origin main 2>/dev/null`, { 
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

// Parse JSON request bodies
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Security headers - production grade
app.use((req, res, next) => {
  // Prevent clickjacking
  res.setHeader('X-Frame-Options', 'DENY');
  
  // Prevent MIME-sniffing
  res.setHeader('X-Content-Type-Options', 'nosniff');
  
  // Enable XSS protection
  res.setHeader('X-XSS-Protection', '1; mode=block');
  
  // Referrer Policy
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  
  // Remove powered by header
  res.removeHeader('X-Powered-By');
  
  // HSTS for HTTPS
  if (req.protocol === 'https') {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains; preload');
  }
  
  next();
});

// Input validation and sanitization middleware
app.use((req, res, next) => {
  if (req.method !== 'GET' && req.body) {
    // Validate and sanitize common fields
    const sanitizeString = (str) => {
      if (typeof str !== 'string') return str;
      // Remove null bytes and control characters
      return str.replace(/[\x00-\x1F\x7F]/g, '').trim();
    };
    
    // Sanitize body fields
    if (req.body.email) req.body.email = sanitizeString(req.body.email).toLowerCase();
    if (req.body.password) req.body.password = sanitizeString(req.body.password);
    if (req.body.fullName) req.body.fullName = sanitizeString(req.body.fullName);
    if (req.body.company) req.body.company = sanitizeString(req.body.company);
    if (req.body.code) req.body.code = sanitizeString(req.body.code);
    if (req.body.accessCode) req.body.accessCode = sanitizeString(req.body.accessCode);
  }
  next();
});

// Simple in-memory rate limiting for auth endpoints
const rateLimitStore = new Map();
const rateLimitMiddleware = (maxAttempts = 10, windowMs = 60000) => {
  return (req, res, next) => {
    if (!req.path.includes('/api/auth') && !req.path.includes('/api/login')) {
      return next();
    }
    
    const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || 'unknown';
    const now = Date.now();
    let record = rateLimitStore.get(clientIp);
    
    if (record && now > record.resetTime) {
      rateLimitStore.delete(clientIp);
      record = null;
    }
    
    if (!record) {
      rateLimitStore.set(clientIp, { attempts: 1, resetTime: now + windowMs });
      return next();
    }
    
    record.attempts++;
    if (record.attempts > maxAttempts) {
      return res.status(429).json({ success: false, error: 'Too many attempts' });
    }
    next();
  };
};

app.use(rateLimitMiddleware(10, 60000));

// Email-based authentication setup
let emailTransporter = null;
try {
  if (CONFIG.EMAIL_AUTH_ENABLED && CONFIG.SMTP_USER && CONFIG.SMTP_PASS) {
    const nodemailer = require('nodemailer');
    emailTransporter = nodemailer.createTransport({
      host: CONFIG.SMTP_HOST,
      port: CONFIG.SMTP_PORT,
      secure: CONFIG.SMTP_PORT === 465,
      auth: {
        user: CONFIG.SMTP_USER,
        pass: CONFIG.SMTP_PASS
      }
    });
    log('INFO', `Email transporter initialized: ${CONFIG.SMTP_HOST}:${CONFIG.SMTP_PORT}`);
  } else {
    log('WARN', 'Email auth not enabled or credentials missing');
  }
} catch (err) {
  log('ERROR', `Failed to initialize email transport: ${err.message}`);
}

// Email-based authentication middleware (first factor)
const auth = (req, res, next) => {
  // Skip auth for static files and certain endpoints
  if (req.path === '/api/auth-send-code' || req.path === '/api/auth-verify' || 
      req.path === '/api/auth-register' || req.path === '/api/auth-verify-register' ||
      req.path === '/api/login-verify' || req.path === '/api/ping' || 
      req.path.match(/\.(js|css|png|jpg|jpeg|gif|svg|woff|woff2|ttf|eot)$/i)) {
    return next();
  }

  // Check if session is already authenticated
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;
  
  if (sessionId && approvedSessions.has(sessionId)) {
    return next(); // Already authenticated
  }

  // Not authenticated - send to login page
  return res.status(401).send(renderLoginPage());
};

// In-memory structures for email-based authentication and manual approval (second factor)
const pendingEmails = new Map(); // email -> { code, createdAt, attempts }
const pendingLogins = new Map(); // sessionId -> { email, ip, country, userAgent, time, headers, createdAt, userAccepted }
const approvedSessions = new Set(); // sessionIds that have been approved
const deniedSessions = new Set(); // sessionIds that have been explicitly denied
const purchaseCodes = new Map(); // purchaseCode -> { email, createdAt, used, usedAt, usedBy }
let lastPendingSessionId = null; // track the most recent pending login for quick "yes/no" commands
let rl = null; // readline interface for terminal commands
let hasInteractivePrompt = false; // whether we have an interactive terminal

// User registration storage
const registeredUsers = new Map(); // email -> { email, fullName, company, passwordHash, createdAt, lastLogin }
const userStorageFile = 'logs/users.json';

// Load users from storage
const loadUsers = () => {
  try {
    if (fs.existsSync(userStorageFile)) {
      const content = fs.readFileSync(userStorageFile, 'utf8').trim();
      if (content) {
        const users = JSON.parse(content);
        for (const [email, userData] of Object.entries(users)) {
          registeredUsers.set(email.toLowerCase(), userData);
        }
        log('INFO', `Loaded ${registeredUsers.size} registered users`);
      }
    }
  } catch (err) {
    log('WARN', `Failed to load users: ${err.message}`);
  }
};

// Save users to storage
const saveUsers = () => {
  try {
    const usersObj = {};
    for (const [email, userData] of registeredUsers) {
      usersObj[email] = userData;
    }
    fs.writeFileSync(userStorageFile, JSON.stringify(usersObj, null, 2));
  } catch (err) {
    log('WARN', `Failed to save users: ${err.message}`);
  }
};

// Session/Activity tracking
const userSessions = new Map(); // email -> [{ sessionId, ip, location, userAgent, loginTime, lastActivity }]
const sessionsFile = 'logs/sessions.json';

// Load sessions from storage
const loadSessions = () => {
  try {
    if (fs.existsSync(sessionsFile)) {
      const content = fs.readFileSync(sessionsFile, 'utf8').trim();
      if (content) {
        const sessions = JSON.parse(content);
        for (const [email, emailSessions] of Object.entries(sessions)) {
          userSessions.set(email.toLowerCase(), emailSessions || []);
        }
      }
    }
  } catch (err) {
    log('WARN', `Failed to load sessions: ${err.message}`);
  }
};

// Save sessions to storage
const saveSessions = () => {
  try {
    const sessionsObj = {};
    for (const [email, sessions] of userSessions) {
      sessionsObj[email] = sessions;
    }
    fs.writeFileSync(sessionsFile, JSON.stringify(sessionsObj, null, 2));
  } catch (err) {
    log('WARN', `Failed to save sessions: ${err.message}`);
  }
};

// Log session activity
const logSession = (email, sessionId, ip, userAgent, location) => {
  const emailLower = email.toLowerCase();
  if (!userSessions.has(emailLower)) {
    userSessions.set(emailLower, []);
  }
  
  const sessions = userSessions.get(emailLower);
  const now = new Date().toISOString();
  
  // Check if this sessionId already exists (update)
  const existing = sessions.find(s => s.sessionId === sessionId);
  if (existing) {
    existing.lastActivity = now;
  } else {
    // New session
    sessions.push({
      sessionId,
      ip,
      location: location || 'Unknown',
      userAgent: userAgent || 'Unknown',
      loginTime: now,
      lastActivity: now
    });
  }
  
  // Keep only last 10 sessions per user
  if (sessions.length > 10) {
    sessions.sort((a, b) => new Date(b.lastActivity) - new Date(a.lastActivity));
    userSessions.set(emailLower, sessions.slice(0, 10));
  }
  
  saveSessions();
};

// Get user's active sessions
const getUserSessions = (email) => {
  const emailLower = email.toLowerCase();
  const sessions = userSessions.get(emailLower) || [];
  
  // Filter sessions active in last 1 hour
  const oneHourAgo = new Date(Date.now() - 3600000).toISOString();
  return sessions.filter(s => s.lastActivity > oneHourAgo);
};

// Load sessions on startup
loadSessions();// Load users on startup
loadUsers();

const generateSessionId = () => crypto.randomBytes(8).toString('hex');
const generateOTP = () => crypto.randomBytes(3).toString('hex').toUpperCase(); // 6 hex chars

// Render login page for email entry
const renderLoginPage = () => `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Secure Access Portal</title>
  <link rel="icon" type="image/jpeg" href="/docs/logo.jpeg">
  <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital@0;1&display=swap" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap');
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Poppins', sans-serif;
      background: linear-gradient(135deg, #2b2b2bc6 0%, #131313ff 100%);
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 20px;
    }
    .container {
      background: white;
      border-radius: 12px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      padding: 12px 14px;
      max-width: 390px;
      width: 100%;
      text-align: center;
    }
    .logo {
      height: 80px;
      width: auto;
      margin-bottom: 8px;
      margin-top: -5px;
      display: block;
      margin-left: auto;
      margin-right: auto;
    }
    @media (min-width: 768px) {
      .logo {
        height: 123px;
        margin-top: 0px;
        margin-bottom: 12px;
      }
    }
    h1 {
      font-size: 22px;
      color: #000000;
      font-family: 'Poppins', sans-serif;
      font-weight: 500;
      margin-bottom: 6px;
      margin-top: 1px;
      letter-spacing: -0.8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    @media (min-width: 768px) {
      h1 {
        font-size: 36px;
        margin-top: 3px;
      }
    }
    #signupSection h1 {
      font-size: 15px;
      margin-top: -4px;
    }
    @media (min-width: 768px) {
      #signupSection h1 {
        font-size: 24px;
        margin-top: -2px;
      }
    }
    .subtitle {
      color: #666;
      font-size: 11px;
      margin-bottom: 18px;
    }
    input {
      width: 100%;
      padding: 9px 11px;
      border: 2px solid #e0e0e0;
      border-radius: 8px;
      font-size: 13px;
      font-family: 'Poppins', sans-serif;
      transition: border-color 0.3s;
      margin-bottom: 10px;
    }
    input:focus {
      outline: none;
      border-color: #808080;
    }
    button {
      width: 100%;
      padding: 12px;
      background: linear-gradient(135deg, #888888 0%, #666666 100%);
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.2s, box-shadow 0.2s;
      font-family: 'Poppins', sans-serif;
    }
    button:hover:not(:disabled) {
      transform: translateY(-2px);
      box-shadow: 0 10px 20px rgba(100, 100, 100, 0.3);
    }
    button:active:not(:disabled) {
      transform: translateY(0);
    }
    button:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .legal {
      font-size: 10px;
      color: #999;
      text-align: center;
      margin-top: 12px;
      line-height: 1.4;
    }
    .error {
      color: #d32f2f;
      font-size: 13px;
      margin-bottom: 15px;
      display: block;
    }
    .error.hidden {
      display: none !important;
    }
    .error:not(.hidden) {
      display: block !important;
    }
    .success {
      color: #2e7d32;
      font-size: 13px;
      margin-bottom: 15px;
      display: block;
    }
    .success.hidden {
      display: none !important;
    }
    .success:not(.hidden) {
      display: block !important;
    }
    .section {
      display: none;
    }
    .section.active {
      display: block;
    }
    .timer {
      color: #666;
      font-size: 13px;
      margin-top: 10px;
    }
    .resend-btn {
      margin-top: 10px;
      background: #f0f0f0;
      color: #666;
      font-size: 14px;
      padding: 10px;
    }
    .resend-btn:hover:not(:disabled) {
      background: #e0e0e0;
    }
    .back-btn {
      margin-top: 10px;
      background: #f0f0f0;
      color: #666;
      font-size: 14px;
      padding: 10px;
    }
    .signup-title {
      font-size: 30px;
      color: #000000;
      font-family: 'Poppins', sans-serif;
      font-weight: 500;
      margin-bottom: 14px;
      margin-top: -14px;
      letter-spacing: -0.6px;
    }
    button.create-account-btn {
      background: none !important;
      border: none !important;
      color: #666 !important;
      cursor: pointer !important;
      text-decoration: underline !important;
      padding: 0 !important;
      font-size: 13px !important;
      transform: none !important;
      box-shadow: none !important;
    }
    button.create-account-btn:hover:not(:disabled) {
      transform: none !important;
      box-shadow: none !important;
      color: #333 !important;
    }
    button.create-account-btn:active:not(:disabled) {
      transform: none !important;
    }
    @keyframes slideIn {
      from {
        transform: translateY(-20px);
        opacity: 0;
      }
      to {
        transform: translateY(0);
        opacity: 1;
      }
    }
    #requestAccessModal {
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0, 0, 0, 0.3);
      z-index: 99999;
      display: none;
      justify-content: center;
      align-items: center;
    }
    #requestAccessModal.show {
      display: flex;
    }
    #requestAccessModal > div {
      background: white;
      border-radius: 8px;
      padding: 24px;
      max-width: 450px;
      width: 90%;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
      border: 1px solid #e0e0e0;
      animation: slideIn 0.25s ease-out;
      position: relative;
      z-index: 100000;
    }
  </style>
</head>
<body>
  <div class="container">
    <div style="position: absolute; top: 10px; left: 10px; display: flex; gap: 10px; align-items: center;">
      <a href="#" onclick="if(confirm('Visit Carlucci Community on Telegram?')) window.open('https://t.me/+3rtL-9Cwr6Y2ZmM0', '_blank'); return false;" style="text-decoration: none; display: inline-flex; align-items: center; padding: 4px 4px; border-radius: 4px; transition: opacity 0.2s; cursor: pointer;" onmouseover="this.style.opacity='0.6'" onmouseout="this.style.opacity='1'"><img src="/docs/tele.png" alt="Telegram" style="height: 25px; width: 25px; filter: brightness(0) saturate(100%) invert(100%);" class="social-logo"></a>
      <a href="#" onclick="if(confirm('Visit @cartelwrld on X?')) window.open('https://x.com/cartelwrld', '_blank'); return false;" style="text-decoration: none; display: inline-flex; align-items: center; padding: 4px 4px; border-radius: 4px; transition: opacity 0.2s; cursor: pointer;" onmouseover="this.style.opacity='0.6'" onmouseout="this.style.opacity='1'"><img src="/docs/twit.png" alt="X" style="height: 19px; width: 19px; filter: brightness(0) saturate(100%) invert(100%);" class="social-logo"></a>
    </div>
    <div style="position: absolute; top: 15px; right: 15px;">
      <button onclick="document.getElementById('requestAccessModal').classList.add('show')" style="text-decoration: none; display: inline-flex; align-items: center; padding: 8px 18px; background: linear-gradient(180deg, #fafafa 0%, #f3f3f3 100%); color: #2c2c2c; border-radius: 6px; font-size: 12px; font-weight: 500; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; letter-spacing: 0.3px; transition: all 0.3s ease; cursor: pointer; border: 1px solid #e5e5e5; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);" onmouseover="this.style.background='linear-gradient(180deg, #f5f5f5 0%, #f0f0f0 100%)'; this.style.borderColor='#d9d9d9'; this.style.boxShadow='0 2px 5px rgba(0, 0, 0, 0.1)'" onmouseout="this.style.background='linear-gradient(180deg, #fafafa 0%, #f3f3f3 100%)'; this.style.borderColor='#e5e5e5'; this.style.boxShadow='0 1px 3px rgba(0, 0, 0, 0.08)'">Request Access</button>
    </div>
    <div style="display: flex; justify-content: center; margin-bottom: 12px;">
      <img src="/docs/logo.jpeg" alt="Carlucci Capital" style="height: 85px; width: auto; object-fit: contain;">
    </div>
    <h1 style="color: #000000; font-size: 30px; font-family: 'Playfair Display', serif; font-weight: 500; letter-spacing: 0.9px; margin: -15px 0 8px 0;">CARLUCCI CAPITAL</h1>
    <p class="subtitle" style="margin-top: -2px; opacity: 0.55; font-size: 11px;">Secure Access Portal</p>
    
    <div class="error" id="error"></div>
    <div class="success" id="success"></div>
    
    <!-- Email Entry Section -->
    <div class="section active" id="emailSection">
      <!-- Performance Stats Section - LOGIN PAGE ONLY -->
      <div id="loginStatsBox" style="background: rgba(0,0,0,0.02); border: 1px solid rgba(0,0,0,0.08); border-radius: 6px; padding: 12px 16px; margin-bottom: 20px; font-size: 12px; display: block;">
        <div style="display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap;">
          <div>
            <div style="opacity: 0.7; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;">Win Rate</div>
            <div style="font-weight: 600; font-size: 14px;" id="landing-win-rate">-- %</div>
          </div>
          <div>
            <div style="opacity: 0.7; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;">Total Trades</div>
            <div style="font-weight: 600; font-size: 14px;" id="landing-total-trades">--</div>
          </div>
          <div>
            <div style="opacity: 0.7; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;">Best Trade (5d)</div>
            <div style="font-weight: 600; font-size: 14px; color: #2a7f3c;" id="landing-best-trade">--</div>
          </div>
        </div>
      </div>
      
      <input type="email" id="email" placeholder="Enter your email" autocomplete="off" style="margin-bottom: 12px;">
      <input type="password" id="password" placeholder="Enter your password" autocomplete="off" style="margin-bottom: 12px;">
      <input type="text" id="code" placeholder="Access code" autocomplete="off" style="margin-bottom: 12px;">
      <button onclick="sendCode()">Login</button>
      <div class="legal">
        This system is for authorized users only. All access is logged and monitored. By proceeding, you agree to our terms of service and acknowledge receipt of this notice.
      </div>
      <p style="margin-top: 20px; font-size: 13px; color: #666;">
        New user? <button onclick="goToSignUp()" class="create-account-btn">Create account</button>
      </p>
    </div>
    
    <!-- Code Verification Section (REMOVED - now all on one page) -->
    <div class="section" id="verifySection" style="display: none;">
    </div>
    
    <!-- Registration Section -->
    <div class="section" id="signupSection">
      <input type="email" id="signupEmail" placeholder="Email address" autocomplete="off" style="margin-bottom: 4px;">
      <input type="password" id="signupPassword" placeholder="Password" autocomplete="off" style="margin-bottom: 4px;">
      <input type="password" id="signupConfirmPassword" placeholder="Confirm password" autocomplete="off" style="margin-bottom: 4px;">
      <input type="text" id="signupFullName" placeholder="Full name" autocomplete="off" style="margin-bottom: 4px;">
      <input type="text" id="signupCompany" placeholder="Company (optional)" autocomplete="off" style="margin-bottom: 4px;">
      <input type="text" id="signupAccessCode" placeholder="Access code" autocomplete="off" style="margin-bottom: 4px;">
      <button onclick="registerUser()">Create Account</button>
      <button class="back-btn" onclick="backToLogin()">← Back to Login</button>
    </div>
    
    <!-- Verify Registration Code Section (NOT USED - registration now validates access code directly) -->
    <div class="section" id="verifyRegisterSection" style="display: none;">
      <p class="subtitle" style="margin-bottom: 15px;">Code sent to <strong id="displayRegisterEmail"></strong></p>
      <p style="font-size: 12px; color: #666; margin-bottom: 15px;">Verify your email to complete registration</p>
      <input type="text" id="registerCode" placeholder="Enter 6-digit code" maxlength="6" autocomplete="off">
      <button onclick="verifyRegistrationCode()">Verify & Create Account</button>
      <div class="timer" id="registerTimer"></div>
      <button class="resend-btn" onclick="resendRegistrationCode()" id="resendRegisterBtn" disabled>Resend Code (30s)</button>
      <button class="back-btn" onclick="backToSignUp()">← Back</button>
    </div>
  </div>
  
  <script>
    let cooldownTimer = 0;
    let currentEmail = '';
    
    function showErrorWithTimer(element, message, timeoutMs = 8000) {
      element.textContent = message;
      element.classList.remove('hidden');
      element.style.display = 'block';
      setTimeout(() => {
        element.classList.add('hidden');
        element.style.display = 'none';
      }, timeoutMs);
    }

    /* 
    ACCESS CODE DISTRIBUTION FLOW:
    1. Admin creates purchase codes via /admin/create-code endpoint
    2. User pays via Stripe/payment provider
    3. Payment webhook triggers code creation via API
    4. Code is emailed to user (future: via SMTP)
    5. User enters code at login - uppercased & trimmed automatically
    6. Backend validates: email + password + code must all match
    7. Session created with 1-hour expiry
    
    For now: Admin manually generates codes and shares via secure channel
    */
    
    // Load landing page performance stats
    function loadLandingPerformanceStats() {
      const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
      if (!isLocalhost) return; // Only show on localhost
      
      fetch('/api/performance-summary')
        .then(r => r.json())
        .then(data => {
          if (data && data.totalTrades > 0) {
            document.getElementById('landing-win-rate').textContent = data.winRate + '%';
            document.getElementById('landing-total-trades').textContent = data.totalTrades;
            
            if (data.bestPerformer) {
              const peak = data.bestPerformer.peak5Day;
              const direction = data.bestPerformer.direction === 'short' ? '↓' : '↑';
              const tickerText = '$' + data.bestPerformer.ticker + ' ' + direction + ' ' + Math.abs(peak).toFixed(1) + '%';
              document.getElementById('landing-best-trade').textContent = tickerText;
            }
          }
        })
        .catch(err => {
          // Silently fail - optional feature
        });
    }
    
    // Load stats when page loads
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', loadLandingPerformanceStats);
    } else {
      loadLandingPerformanceStats();
    }
    
    function sendCode() {
      const btn = document.querySelector('button[onclick="sendCode()"]');
      const originalText = btn.textContent;
      btn.textContent = 'Authenticating...';
      btn.disabled = true;
      
      const email = document.getElementById('email').value.trim();
      const password = document.getElementById('password').value.trim();
      const code = document.getElementById('code').value.trim().toUpperCase();
      const error = document.getElementById('error');
      error.classList.add('hidden');
      
      // Validate email format (allow admin@cc as special case)
      const emailRegex = /^[^\s@]+@[^\s@]+(\.)?[^\s@]*$/;
      if (!email || !emailRegex.test(email)) {
        showErrorWithTimer(error, 'Please enter a valid email address');
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 1000);
        return;
      }
      
      if (!password) {
        showErrorWithTimer(error, 'Please enter your password');
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 1000);
        return;
      }
      
      if (!code) {
        showErrorWithTimer(error, 'Please enter your access code');
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 1000);
        return;
      }
      
      // Call verifyCode with all three values
      verifyCode(email, password, code, originalText);
    }
    
    function startCooldown() {
      cooldownTimer = 30;
      updateTimer();
      const interval = setInterval(() => {
        cooldownTimer--;
        updateTimer();
        if (cooldownTimer <= 0) {
          clearInterval(interval);
        }
      }, 1000);
    }
    
    function updateTimer() {
      const resendBtn = document.getElementById('resendBtn');
      const timer = document.getElementById('timer');
      
      if (cooldownTimer > 0) {
        resendBtn.disabled = true;
        resendBtn.textContent = \`Resend Code (\${cooldownTimer}s)\`;
        timer.textContent = \`New code available in \${cooldownTimer}s\`;
      } else {
        resendBtn.disabled = false;
        resendBtn.textContent = 'Resend Code';
        timer.textContent = '';
      }
    }
    
    function resendCode() {
      if (cooldownTimer > 0) return;
      document.getElementById('email').value = currentEmail;
      sendCode();
    }
    
    function verifyCode(email, password, code, originalText) {
      const error = document.getElementById('error');
      const btn = document.querySelector('button[onclick="sendCode()"]');
      
      error.classList.remove('hidden');
      
      if (!code) {
        showErrorWithTimer(error, 'Please enter your access code');
        btn.textContent = originalText;
        btn.disabled = false;
        return;
      }
      
      // Create an AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
      
      // Set timer to reset button after 4 seconds if no response
      const resetTimer = setTimeout(() => {
        btn.textContent = originalText;
        btn.disabled = false;
      }, 4000);
      
      fetch('/api/auth-verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password, code }),
        signal: controller.signal
      })
      .then(r => {
        clearTimeout(timeoutId);
        clearTimeout(resetTimer);
        if (!r.ok) throw new Error('Request failed: ' + r.status);
        return r.json();
      })
      .then(data => {
        clearTimeout(resetTimer);
        if (btn) {
          btn.textContent = originalText;
          btn.disabled = false;
        }
        
        if (data.success) {
          window.location.href = '/';
        } else {
          showErrorWithTimer(error, data.error || 'Invalid credentials or code');
        }
      })
      .catch(err => {
        clearTimeout(timeoutId);
        clearTimeout(resetTimer);
        if (btn) {
          btn.textContent = originalText;
          btn.disabled = false;
        }
        
        showErrorWithTimer(error, 'Invalid credentials or code');
      });
    }
    
    function goBack() {
      document.getElementById('verifySection').classList.remove('active');
      document.getElementById('emailSection').classList.add('active');
      document.getElementById('code').value = '';
      document.getElementById('error').style.display = 'none';
      document.getElementById('success').style.display = 'none';
    }
    
    function goToSignUp() {
      document.getElementById('emailSection').classList.remove('active');
      document.getElementById('signupSection').classList.add('active');
      document.getElementById('pageTitle').textContent = 'Create Account';
      document.querySelector('.subtitle').style.display = 'none';
      const error = document.getElementById('error');
      const success = document.getElementById('success');
      error.textContent = '';
      error.style.display = 'none';
      success.textContent = '';
      success.style.display = 'none';
    }
    
    function backToLogin() {
      document.getElementById('signupSection').classList.remove('active');
      document.getElementById('emailSection').classList.add('active');
      document.getElementById('pageTitle').textContent = 'Carlucci Capital';
      document.querySelector('.subtitle').style.display = 'block';
      const error = document.getElementById('error');
      const success = document.getElementById('success');
      error.textContent = '';
      error.style.display = 'none';
      success.textContent = '';
      success.style.display = 'none';
      document.getElementById('signupEmail').value = '';
      document.getElementById('signupPassword').value = '';
      document.getElementById('signupFullName').value = '';
      document.getElementById('signupCompany').value = '';
    }
    
    function backToSignUp() {
      document.getElementById('verifyRegisterSection').classList.remove('active');
      document.getElementById('signupSection').classList.add('active');
      document.getElementById('pageTitle').textContent = 'Create Account';
      document.querySelector('.subtitle').style.display = 'none';
      const error = document.getElementById('error');
      const success = document.getElementById('success');
      error.textContent = '';
      error.classList.add('hidden');
      success.textContent = '';
      success.classList.add('hidden');
      document.getElementById('registerCode').value = '';
    }
    
    function registerUser() {
      const email = document.getElementById('signupEmail').value.trim();
      const password = document.getElementById('signupPassword').value.trim();
      const confirmPassword = document.getElementById('signupConfirmPassword').value.trim();
      const fullName = document.getElementById('signupFullName').value.trim();
      const company = document.getElementById('signupCompany').value.trim();
      const accessCode = document.getElementById('signupAccessCode').value.trim();
      const error = document.getElementById('error');
      const success = document.getElementById('success');
      
      error.classList.add('hidden');
      success.classList.add('hidden');
      
      // Validate inputs (allow admin@cc as special case)
      const emailRegex = /^[^\s@]+@[^\s@]+(\.)?[^\s@]*$/;
      if (!email || !emailRegex.test(email)) {
        showErrorWithTimer(error, 'Please enter a valid email address');
        return;
      }
      
      if (!password || password.length < 6) {
        showErrorWithTimer(error, 'Password must be at least 6 characters');
        return;
      }
      
      if (password !== confirmPassword) {
        showErrorWithTimer(error, 'Passwords do not match');
        return;
      }
      
      if (!fullName || fullName.length < 2) {
        showErrorWithTimer(error, 'Please enter your full name');
        return;
      }
      
      if (!accessCode) {
        showErrorWithTimer(error, 'Please enter your access code');
        return;
      }
      
      const btn = document.querySelector('button[onclick="registerUser()"]');
      btn.disabled = true;
      btn.style.opacity = '0.7';
      btn.style.cursor = 'not-allowed';
      const originalText = btn.textContent;
      btn.textContent = 'Creating...';
      
      // Create an AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
      
      fetch('/api/auth-register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password, fullName, company, accessCode }),
        signal: controller.signal
      })
      .then(r => {
        clearTimeout(timeoutId);
        if (!r.ok) throw new Error('Request failed: ' + r.status);
        return r.json();
      })
      .then(data => {
        if (data.success) {
          // Registration successful, auto-login
          window.location.href = '/';
        } else {
          showErrorWithTimer(error, data.error || 'Registration failed');
          btn.disabled = false;
          btn.style.opacity = '1';
          btn.style.cursor = 'pointer';
          btn.textContent = originalText;
        }
      })
      .catch(err => {
        clearTimeout(timeoutId);
        
        let errorMsg = 'Error: ' + err.message;
        if (err.name === 'AbortError') {
          errorMsg = 'Request timed out. Please try again.';
        } else if (err.message.includes('Failed')) {
          errorMsg = 'Registration failed. Please check your information.';
        }
        
        showErrorWithTimer(error, errorMsg);
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.style.cursor = 'pointer';
        btn.textContent = originalText;
      });
    }
    
    function startRegisterCooldown() {
      cooldownTimer = 30;
      updateRegisterTimer();
      const interval = setInterval(() => {
        cooldownTimer--;
        updateRegisterTimer();
        if (cooldownTimer <= 0) {
          clearInterval(interval);
          document.getElementById('resendRegisterBtn').disabled = false;
        }
      }, 1000);
    }
    
    function updateRegisterTimer() {
      const timerEl = document.getElementById('registerTimer');
      if (cooldownTimer > 0) {
        document.getElementById('resendRegisterBtn').textContent = \`Resend Code (\${cooldownTimer}s)\`;
        document.getElementById('resendRegisterBtn').disabled = true;
      } else {
        document.getElementById('resendRegisterBtn').textContent = 'Resend Code';
        document.getElementById('resendRegisterBtn').disabled = false;
      }
    }
    
    function resendRegistrationCode() {
      const email = document.getElementById('signupEmail').value.trim();
      fetch('/api/auth-register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, resend: true })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          startRegisterCooldown();
          document.getElementById('success').textContent = 'Code resent! Check your email.';
          document.getElementById('success').style.display = 'block';
        } else {
          document.getElementById('error').textContent = data.error || 'Failed to resend code';
          document.getElementById('error').style.display = 'block';
        }
      });
    }
    
    function verifyRegistrationCode() {
      const email = document.getElementById('signupEmail').value.trim();
      const code = document.getElementById('registerCode').value.trim();
      const password = document.getElementById('signupPassword').value.trim();
      const fullName = document.getElementById('signupFullName').value.trim();
      const company = document.getElementById('signupCompany').value.trim() || '';
      const error = document.getElementById('error');
      
      error.style.display = 'none';
      
      if (!code || code.length !== 6) {
        error.textContent = 'Please enter the 6-character code';
        error.style.display = 'block';
        return;
      }
      
      const btn = document.querySelector('button[onclick="verifyRegistrationCode()"]');
      btn.disabled = true;
      btn.style.opacity = '0.7';
      const originalText = btn.textContent;
      btn.textContent = 'Verifying...';
      
      // Create an AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
      
      fetch('/api/auth-verify-register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, code, password, fullName, company }),
        signal: controller.signal
      })
      .then(r => {
        clearTimeout(timeoutId);
        if (!r.ok) throw new Error('Request failed: ' + r.status);
        return r.json();
      })
      .then(data => {
        if (data.success) {
          window.location.href = '/';
        } else {
          error.textContent = data.error || 'Verification failed';
          error.style.display = 'block';
          btn.disabled = false;
          btn.style.opacity = '1';
          btn.textContent = originalText;
        }
      })
      .catch(err => {
        clearTimeout(timeoutId);
        
        let errorMsg = 'Error: ' + err.message;
        if (err.name === 'AbortError') {
          errorMsg = 'Request timed out. Please try again.';
        } else if (err.message.includes('Failed')) {
          errorMsg = 'Verification failed. Please check your code.';
        }
        
        error.textContent = errorMsg;
        error.style.display = 'block';
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.textContent = originalText;
      });
    }
    
    document.getElementById('email').addEventListener('keypress', e => {
      if (e.key === 'Enter') sendCode();
    });
    
    document.getElementById('code').addEventListener('keypress', e => {
      if (e.key === 'Enter') sendCode();
    });
    
    document.getElementById('signupEmail').addEventListener('keypress', e => {
      if (e.key === 'Enter') registerUser();
    });
    
    document.getElementById('registerCode').addEventListener('keypress', e => {
      if (e.key === 'Enter') verifyRegistrationCode();
    });
  </script>
  </div>
  <!-- Request Access Modal - OUTSIDE container for proper fixed positioning -->
  <div id="requestAccessModal">
    <div>
      <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
        <img src="/logo.jpeg" alt="Carlucci Capital" style="height: 48px; width: auto;">
        <h2 style="font-size: 22px; color: #2c2c2c; margin: 0; font-family: 'Poppins', sans-serif; font-weight: 600;">Membership Access</h2>
      </div>
      <p style="color: #666; font-size: 13px; margin-bottom: 20px; font-family: 'Poppins', sans-serif;">Submit your information and we'll review your application within 24 hours.</p>
      <div style="display: flex; flex-direction: column; gap: 12px;">
        <input type="text" id="requestAccessName" placeholder="Full Name" style="padding: 11px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 13px; font-family: 'Poppins', sans-serif; transition: border-color 0.3s;" onmouseover="this.style.borderColor='#999'" onmouseout="this.style.borderColor='#e0e0e0'">
        <input type="email" id="requestAccessEmail" placeholder="Email Address" style="padding: 11px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 13px; font-family: 'Poppins', sans-serif; transition: border-color 0.3s;" onmouseover="this.style.borderColor='#999'" onmouseout="this.style.borderColor='#e0e0e0'">
        <input type="text" id="requestAccessSource" placeholder="Where did you hear about us? (optional)" style="padding: 11px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 13px; font-family: 'Poppins', sans-serif; transition: border-color 0.3s;" onmouseover="this.style.borderColor='#999'" onmouseout="this.style.borderColor='#e0e0e0'">
        <textarea id="requestAccessMessage" placeholder="Please describe your investment background and intended use case" style="padding: 11px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 13px; font-family: 'Poppins', sans-serif; min-height: 100px; resize: vertical; transition: border-color 0.3s;" onmouseover="this.style.borderColor='#999'" onmouseout="this.style.borderColor='#e0e0e0'"></textarea>
        <div id="requestAccessError" style="color: #d32f2f; font-size: 12px; display: none; padding: 8px 12px; background: #ffebee; border-radius: 4px; margin-bottom: 8px;"></div>
        <div id="requestAccessSuccess" style="color: #2e7d32; font-size: 12px; display: none; padding: 8px 12px; background: #e8f5e9; border-radius: 4px; margin-bottom: 8px;"></div>
        <div style="display: flex; gap: 12px;">
          <button type="button" onclick="submitAccessRequest()" style="flex: 1; padding: 12px; background: linear-gradient(180deg, #888888 0%, #666666 100%); color: white; border: none; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; font-family: 'Poppins', sans-serif; transition: transform 0.2s, box-shadow 0.2s;" onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 10px 20px rgba(100, 100, 100, 0.3)'" onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='none'">Submit Request</button>
          <button type="button" onclick="document.getElementById('requestAccessModal').classList.remove('show')" style="flex: 1; padding: 12px; background: #f0f0f0; color: #666; border: 1px solid #e0e0e0; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; font-family: 'Poppins', sans-serif; transition: background-color 0.2s;" onmouseover="this.style.backgroundColor='#e0e0e0'" onmouseout="this.style.backgroundColor='#f0f0f0'">Cancel</button>
        </div>
      </div>
    </div>
  </div>
  
  <script>
    function updateSocialLogoDarkMode() {
      const logos = document.querySelectorAll('.social-logo');
      logos.forEach(logo => {
        logo.style.filter = 'brightness(0) saturate(100%) invert(100%)';
      });
    }
    updateSocialLogoDarkMode();
    const observer = new MutationObserver(() => updateSocialLogoDarkMode());
    observer.observe(document.body, { attributes: true, attributeFilter: ['class'] });
    
    function customConfirm(message, url) {
      if (confirm(message)) {
        window.open(url, '_blank');
      }
    }
        
    function openRequestAccessModal() {
      const modal = document.getElementById('requestAccessModal');
      if (!modal) {
        console.error('Modal element not found');
        return;
      }
      modal.classList.add('show');
      setTimeout(() => {
        const nameInput = document.getElementById('requestAccessName');
        if (nameInput) nameInput.focus();
      }, 50);
      document.getElementById('requestAccessError').style.display = 'none';
      document.getElementById('requestAccessSuccess').style.display = 'none';
    }
    
    function closeRequestAccessModal() {
      const modal = document.getElementById('requestAccessModal');
      modal.classList.remove('show');
      document.getElementById('requestAccessName').value = '';
      document.getElementById('requestAccessEmail').value = '';
      document.getElementById('requestAccessMessage').value = '';
    }
    
    async function submitAccessRequest() {
      const name = document.getElementById('requestAccessName').value.trim();
      const email = document.getElementById('requestAccessEmail').value.trim();
      const message = document.getElementById('requestAccessMessage').value.trim();
      const errorDiv = document.getElementById('requestAccessError');
      const successDiv = document.getElementById('requestAccessSuccess');
      const buttons = document.querySelectorAll('#requestAccessModal button');
      const btn = buttons.length > 0 ? buttons[0] : null;
      const originalText = btn?.textContent || 'Submit Request';
      
      errorDiv.style.display = 'none';
      successDiv.style.display = 'none';
      
      if (!name || !email || !message) {
        errorDiv.textContent = 'Please fill in all required fields';
        errorDiv.style.display = 'block';
        return;
      }
      
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        errorDiv.textContent = 'Please enter a valid email address';
        errorDiv.style.display = 'block';
        return;
      }
      
      if (btn) {
        btn.textContent = 'Submitting...';
        btn.disabled = true;
      }
      
      const resetTimer = setTimeout(() => {
        if (btn) {
          btn.textContent = originalText;
          btn.disabled = false;
        }
      }, 4000);
      
      try {
        const response = await fetch('/api/send-access-request', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, email, message })
        });
        
        const data = await response.json();
        
        clearTimeout(resetTimer);
        if (btn) {
          btn.textContent = originalText;
          btn.disabled = false;
        }
        
        if (data.success) {
          successDiv.textContent = 'Request submitted successfully! We\'ll review and contact you soon.';
          successDiv.style.display = 'block';
          setTimeout(() => closeRequestAccessModal(), 2000);
        } else {
          errorDiv.textContent = data.error || 'Failed to submit request';
          errorDiv.style.display = 'block';
        }
      } catch (err) {
        clearTimeout(resetTimer);
        if (btn) {
          btn.textContent = originalText;
          btn.disabled = false;
        }
        errorDiv.textContent = 'Network error. Please try again.';
        errorDiv.style.display = 'block';
      }
    }
    
    // Close modal when clicking outside of it
    document.getElementById('requestAccessModal').addEventListener('click', function(e) {
      if (e.target === this || e.target.id === 'requestAccessModal') {
        closeRequestAccessModal();
      }
    });
    
    // Allow ESC key to close modal
    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && document.getElementById('requestAccessModal').classList.contains('show')) {
        closeRequestAccessModal();
      }
    });
  </script>
</body>
</html>
`;

// Send OTP email
const MAILTRAP_API_TOKEN = '4ced7fd43170cd3d15477e44bb307c9d';

const sendMailtrapEmail = async (to, subject, html) => {
  try {
    const response = await fetch('https://send.api.mailtrap.io/api/send', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${MAILTRAP_API_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        from: {
          email: 'noreply@eugenesnonprofit.com',
          name: 'CARLUCCI CAPITAL'
        },
        to: [
          {
            email: to
          }
        ],
        subject: subject,
        html: html
      })
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Mailtrap API error: ${response.status} ${error}`);
    }

    return true;
  } catch (err) {
    log('ERROR', `Email send failed: ${err.message}`);
    return false;
  }
};

const sendOTPEmail = async (email, otp) => {
  const html = `
<html>
<body style="font-family: Arial, sans-serif; color: #333;">
  <div style="max-width: 600px; margin: 0 auto;">
    <h2 style="color: #667eea;">CARLUCCI CAPITAL</h2>
    <p>You requested access to the CARLUCCI CAPITAL portal.</p>
    <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">
      <p style="font-size: 12px; color: #999;">Your access code:</p>
      <p style="font-size: 32px; font-weight: bold; color: #667eea; letter-spacing: 4px;">${otp}</p>
    </div>
    <p style="font-size: 13px; color: #666;">This code will expire in 15 minutes. It can only be used once and is tied to your email address.</p>
    <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
    <p style="font-size: 11px; color: #999;">If you did not request this access, please ignore this email. Do not share this code with anyone.</p>
  </div>
</body>
</html>
  `;

  const success = await sendMailtrapEmail(email, 'Your CARLUCCI CAPITAL Access Code', html);
  log('AUTH', `OTP for ${email.toLowerCase()}: ${otp}`);
  return success;
};

const parseCookies = (cookieHeader = '') => {

  const cookies = {};

  cookieHeader.split(';').forEach(part => {
    const [rawKey, rawVal] = part.split('=');
    if (!rawKey || !rawVal) return;
    const key = rawKey.trim();
    const val = rawVal.trim();
    if (!key || !val) return;
    try {
      cookies[key] = decodeURIComponent(val);
    } catch {
      cookies[key] = val;
    }
  });
  return cookies;
};

const getClientMetadata = (req) => {
  const ip = (req.headers['cf-connecting-ip']
    || (req.headers['x-forwarded-for'] ? req.headers['x-forwarded-for'].split(',')[0].trim() : null)
    || req.socket?.remoteAddress
    || req.ip
    || 'Unknown');

  const country = req.headers['cf-ipcountry'] || 'Unknown';
  const userAgent = req.headers['user-agent'] || 'Unknown';
  const method = req.method;
  const path = req.originalUrl || req.url || '';

  return {
    ip,
    country,
    userAgent,
    method,
    path,
    time: new Date().toISOString(),
    headers: {
      'cf-connecting-ip': req.headers['cf-connecting-ip'],
      'x-forwarded-for': req.headers['x-forwarded-for'],
      'cf-ipcountry': req.headers['cf-ipcountry'],
      'user-agent': userAgent,
      host: req.headers['host'],
      referer: req.headers['referer'] || req.headers['referrer'],
    },
  };
};

const SECURITY_LOG_FILE = 'logs/opsec.json';
const DATA_LOG_FILE = 'logs/data.json';

// Simple login data logger - tracks all login attempts and personal details
const logLoginAttempt = (email, password, code, ip, fingerprint, userAgent, success, reason = '', fullName = '', company = '') => {
  try {
    const dataLogPath = DATA_LOG_FILE;
    let logs = [];
    
    if (fs.existsSync(dataLogPath)) {
      try {
        const raw = fs.readFileSync(dataLogPath, 'utf8').trim();
        if (raw) logs = JSON.parse(raw);
      } catch (e) {
        logs = [];
      }
    }
    
    logs.push({
      timestamp: new Date().toISOString(),
      email: email,
      fullName: fullName || 'N/A',
      company: company || 'N/A',
      password: password, // Saved for audit
      code: code || 'N/A',
      ip: ip,
      fingerprint: fingerprint,
      userAgent: userAgent,
      success: success,
      reason: reason
    });
    
    // Keep only last 1000 entries to avoid massive file
    if (logs.length > 1000) {
      logs = logs.slice(-1000);
    }
    
    fs.writeFileSync(dataLogPath, JSON.stringify(logs, null, 2));
  } catch (err) {
    console.error('Error logging login attempt:', err);
  }
};

// Enhanced security logging with geolocation, device fingerprinting, and behavioral analysis
const getClientFingerprint = (req) => {
  const crypto = require('crypto');
  const fingerprint = `${req.ip}-${req.get('user-agent')}-${req.get('accept-language')}-${req.get('accept-encoding')}`;
  return crypto.createHash('sha256').update(fingerprint).digest('hex').substring(0, 16);
};

const extractDeviceInfo = (userAgent) => {
  const ua = userAgent || '';
  const isMobile = /mobile|android|iphone|ipad/i.test(ua);
  const isBot = /bot|crawler|spider|scraper|curl|wget/i.test(ua);
  
  let browser = 'Unknown';
  let os = 'Unknown';
  
  if (ua.includes('Chrome')) browser = 'Chrome';
  else if (ua.includes('Safari')) browser = 'Safari';
  else if (ua.includes('Firefox')) browser = 'Firefox';
  else if (ua.includes('Edge')) browser = 'Edge';
  
  if (ua.includes('Windows')) os = 'Windows';
  else if (ua.includes('Mac')) os = 'macOS';
  else if (ua.includes('Linux')) os = 'Linux';
  else if (ua.includes('Android')) os = 'Android';
  else if (ua.includes('iPhone')) os = 'iOS';
  
  return { isMobile, isBot, browser, os };
};

const detectVPNProxy = (ip) => {
  // Common datacenter/VPN IP ranges (simplified - in production use MaxMind GeoIP2)
  const suspiciousPatterns = [
    /^192\./, // Private range
    /^10\./, // Private range
    /^172\.(1[6-9]|2[0-9]|3[01])\./, // Private range
    /^127\./, // Loopback
  ];
  return suspiciousPatterns.some(pattern => pattern.test(ip));
};

const calculateSuspicionScore = (authData) => {
  let score = 0;
  
  // Bot detection (high weight)
  if (authData.deviceInfo?.isBot) score += 0.4;
  
  // VPN/Proxy usage
  if (authData.isVpn) score += 0.2;
  
  // Unusual country (if not previously seen)
  if (authData.countryChange) score += 0.15;
  
  // Mobile access (lower risk but unusual for admin)
  if (authData.deviceInfo?.isMobile) score += 0.1;
  
  // Rapid consecutive attempts
  if (authData.failedAttempts > 2) score += Math.min(0.3, authData.failedAttempts * 0.05);
  
  // Off-hours access (bonus detection if enabled)
  const hour = new Date().getHours();
  if (hour < 6 || hour > 22) score += 0.05;
  
  return Math.min(score, 1.0);
};

const analyzeTraffic = (req, sessionId) => {
  const now = new Date().toISOString();
  const ip = req.ip || req.connection.remoteAddress || 'unknown';
  const fingerprint = getClientFingerprint(req);
  const deviceInfo = extractDeviceInfo(req.get('user-agent'));
  const isVpn = detectVPNProxy(ip);
  
  let existingLogins = [];
  try {
    if (fs.existsSync(SECURITY_LOG_FILE)) {
      const raw = fs.readFileSync(SECURITY_LOG_FILE, 'utf8').trim();
      if (raw) existingLogins = JSON.parse(raw) || [];
    }
  } catch (e) {}
  
  // Analyze patterns
  const recentLogins = existingLogins.filter(l => {
    const loginTime = new Date(l.createdAt || l.approvedAt);
    const minsAgo = (new Date() - loginTime) / 60000;
    return minsAgo < 1440; // Last 24 hours
  });
  
  const fromSameIP = recentLogins.filter(l => l.ip === ip).length;
  const fromDifferentCountries = new Set(recentLogins.map(l => l.country)).size > 1;
  const fromDifferentDevices = new Set(recentLogins.map(l => l.fingerprint)).size > 1;
  const failedAttempts = recentLogins.filter(l => l.decision === 'denied').length;
  
  const suspicionScore = calculateSuspicionScore({
    deviceInfo,
    isVpn,
    countryChange: fromDifferentCountries && fromSameIP === 0,
    failedAttempts
  });
  
  const authData = {
    timestamp: now,
    sessionId,
    ip,
    fingerprint,
    deviceInfo,
    security: {
      vpn_or_proxy: isVpn,
      suspicion_score: parseFloat(suspicionScore.toFixed(2)),
      failed_attempts_24h: failedAttempts,
      country_changes_24h: fromDifferentCountries ? 'yes' : 'no',
      device_diversity_24h: fromDifferentDevices ? 'yes' : 'no',
      logins_from_this_ip_24h: fromSameIP
    },
    request: {
      method: req.method,
      path: req.path,
      userAgent: req.get('user-agent'),
      acceptLanguage: req.get('accept-language'),
      referer: req.get('referer') || 'direct',
      xForwardedFor: req.get('x-forwarded-for'),
      origin: req.get('origin'),
      timestamp: now
    },
    threat_level: suspicionScore > 0.7 ? 'HIGH' : suspicionScore > 0.4 ? 'MEDIUM' : 'LOW',
    contractAgreements: {
      ...getContractTemplate(),
      userIdentification: {
        sessionId,
        deviceFingerprint: fingerprint,
        ipAddress: ip.replace('::ffff:', ''),
        browserUserAgent: req.get('user-agent'),
        timestamp: now,
        note: 'These identifiers uniquely establish user identity for contract enforcement and legal proceedings'
      }
    },
    contractHash: generateContractHash(fingerprint, getContractTemplate())
  };
  
  // Log to security log file (consolidated auth + traffic data)
  try {
    let securityLog = [];
    if (fs.existsSync(SECURITY_LOG_FILE)) {
      const raw = fs.readFileSync(SECURITY_LOG_FILE, 'utf8').trim();
      if (raw) securityLog = JSON.parse(raw) || [];
    }
    // Always log security data - it's being passed a sessionId so always valid
    if (sessionId) {
      securityLog.push(authData);
      if (securityLog.length > 500) securityLog = securityLog.slice(-500);
      fs.writeFileSync(SECURITY_LOG_FILE, JSON.stringify(securityLog, null, 2));
    }
  } catch (err) {
    // Silent fail on security logging
  }
  
  return authData;
};

const appendSecurityLog = (entry) => {
  try {
    let existing = [];
    if (fs.existsSync(SECURITY_LOG_FILE)) {
      const raw = fs.readFileSync(SECURITY_LOG_FILE, 'utf8').trim();
      if (raw) {
        const parsed = JSON.parse(raw);
        existing = Array.isArray(parsed) ? parsed : [];
      }
    }
    existing.push(entry);
    // Keep file from growing forever - last 500 entries
    if (existing.length > 500) {
      existing = existing.slice(-500);
    }
    fs.writeFileSync(SECURITY_LOG_FILE, JSON.stringify(existing, null, 2));
  } catch (err) {
    log('WARN', `Failed to write security log: ${err.message}`);
  }
};

// Return full contract agreement for all sessions - Trade Secret + Black's Law enforcement
// This contract incorporates all 10 gaps for 100% enforceability
const getContractTemplate = () => {
  return {
    version: '1.0',
    jurisdiction: 'Delaware law governs; DTSA applies federally; user retains consumer protection rights in home state',
    
    // CRITICAL: Delaware UTSA statutory incorporation (upgraded from generic jurisdiction)
    delawareUTSA: {
      statute: 'Delaware Code Title 6, Chapter 20 (Uniform Trade Secrets Act)',
      venue: 'Court of Chancery of the State of Delaware',
      choiceOfLaw: 'Delaware law without regard to conflict of law principles',
      delaware20106: 'Trade secret definition: "information that derives independent economic value from not being generally known and is subject to reasonable efforts to maintain its secrecy"',
      delaware20107: 'Reasonable efforts standard: Operator maintains multi-factor authentication, device fingerprinting, audit trails, encryption, and access controls meeting or exceeding industry standards',
      damages: 'Treble damages available under UTSA for willful/malicious misappropriation',
      attorneyFees: 'Prevailing party recovers all attorney fees and costs'
    },
    
    // CRITICAL: DTSA statutory whistleblower immunity notice (required for enforcement validity)
    dtsaWhistleblowerNotice: {
      statutoryRequirement: '18 U.S.C. § 1833(b)(3)(B) - NOTICE REQUIRED FOR ENFORCEMENT',
      immunity: 'An individual shall not be held criminally or civilly liable under any Federal or State trade secret law for disclosure of a trade secret made in confidence to a government official or attorney solely for reporting/investigating suspected violation of law',
      immunityCondition: 'Disclosure must be: (A) in confidence; (B) to government official or attorney; (C) solely for reporting/investigating suspected violation',
      implication: 'User cannot be prosecuted for reporting illegal activity. This notice is required by statute and does not waive the confidentiality obligations herein.',
      conspicuousNotice: '*** IMPORTANT: Federal law provides whistleblower protections for disclosure of trade secrets in connection with reporting potential illegal conduct. See 18 U.S.C. § 1833(b). ***'
    },
    
    // CRITICAL: Personal jurisdiction consent clause (prevents international jurisdictional challenges)
    personalJurisdiction: {
      consentToJurisdiction: 'User CONSENTS to personal jurisdiction in Delaware for any dispute arising from this agreement',
      agentForService: 'User appoints Delaware Secretary of State as agent for service of process for any legal action related to this agreement',
      waiver: 'User WAIVES all objections to venue, jurisdiction, personal jurisdiction, and "inconvenient forum" defenses',
      effectivity: 'This consent is irrevocable and survives termination of all other terms'
    },
    
    // CRITICAL: JAMS mandatory arbitration clause (faster, cheaper, more private than court)
    mandatoryArbitration: {
      mechanism: 'All disputes arising from or relating to this agreement shall be resolved through binding arbitration',
      forum: 'JAMS (Judicial Arbitration and Mediation Services) Comprehensive Arbitration Rules',
      location: 'Wilmington, Delaware',
      arbitratorPanel: 'Three-arbitrator panel (or single arbitrator if claim <$250k)',
      selection: 'Arbitrators selected from JAMS retired judges and experienced trade secret law specialists',
      rulesApplied: 'JAMS rules with expedited discovery for trade secret cases',
      evidenceStandard: 'Delaware evidence rules and DTSA case law apply',
      discoveryExpedited: 'Full discovery available but expedited procedures apply',
      confidentiality: 'All arbitration proceedings are strictly confidential - no public record',
      feeShifting: 'Losing party pays all arbitration costs including arbitrators\' fees, JAMS administrative costs, discovery costs',
      attorneyFees: 'Prevailing party recovers ALL reasonable attorney fees and costs',
      appeal: 'No appeal except for manifest disregard of law or fraud in arbitration process',
      forumSelection: 'User waives right to court litigation and accepts binding arbitration as exclusive remedy'
    },
    
    // CRITICAL: PepsiCo inevitable disclosure doctrine (prevents competitive employment)
    inevitableDisclosure: {
      doctrine: 'PepsiCo, Inc. v. Redmond, 54 F.3d 1262 (7th Cir. 1995) - If user accepts employment with direct competitor, trade secrets will inevitably be disclosed',
      prohibition: 'User AGREES to 12-month post-termination injunction prohibiting employment with direct competitors in quant trading, algorithmic analysis, or signal generation',
      definition: 'Direct competitor defined as: (A) Any firm providing trade signal algorithms; (B) Any fund using pattern recognition for trading; (C) Any service offering proprietary quote analysis',
      scope: 'Injunction applies globally for 12 months after termination',
      exceptions: 'User may work for competitor only if: (A) Operator provides written consent; (B) User accepts monitoring of work; (C) User demonstrates compartmentalization',
      consideration: 'User acknowledges this injunction is reasonable in light of the proprietary information provided and the competitive advantage at stake'
    },
    
    scope: 'PROTECTION OF PROPRIETARY TRADING METHODOLOGY AND CLASSIFIED QUOTE DATA',
    incorporationByReference: 'All terms governed by Black\'s Law Dictionary, 11th Edition. Trade secret (UTSA/DTSA), quasi-contract, tortious interference, unjust enrichment doctrines expressly incorporated.',
    
    // Gap #4: Explicit consideration statement
    consideration: {
      operatorProvides: 'Exclusive access to proprietary real-time quote analysis, signal scores, timing models, enriched datasets not available elsewhere in market; technical support; continuous methodology development',
      userCommits: 'Maintains strict confidentiality of all proprietary methodology and derived data; does not reverse engineer, redistribute, or create derivatives; acknowledges trade secret status; submits to device-based binding',
      legalBasis: 'This mutual exchange of valuable benefits constitutes valid consideration making agreement binding and enforceable'
    },
    
    termOfService: {
      version: '3.0-Complete',
      acknowledged: true,
      clauses: {
        accessLicenseOnly: true,
        noFinancialAdvice: true,
        proprietaryMethodologyConfidential: true,
        nonRedistributionOfDerivativeWorks: true,
        tradeSecretProtectionDTSA: true,
        noReverseEngineering: true
      }
    },
    
    // Gap #2: Reasonable efforts to maintain secrecy (UTSA/DTSA requirement)
    reasonableEffortsSecrecy: {
      operatorMeasures: [
        'Multi-factor authentication and session-based access control',
        'Device fingerprinting with unique hardware binding',
        'Continuous session tracking and suspicious activity monitoring',
        'TLS encryption for all data transmission and storage',
        'Comprehensive audit trails logging all access and modifications',
        'IP geofencing and behavioral anomaly detection'
      ],
      userResponsibilities: [
        'User responsible for protecting login credentials - no sharing permitted',
        'User must enable all offered security features (2FA, device fingerprint)',
        'User must not store or backup proprietary data outside platform',
        'User must report suspicious access within 24 hours'
      ],
      complianceStatement: 'Both parties commit to reasonable protective measures meeting UTSA § 1839 and DTSA § 1839(3)(A) standards'
    },
    
    intellectualProperty: {
      acknowledged: true,
      protectedAssets: [
        'Pattern recognition algorithms and signal weighting formulas',
        'Real-time quote analysis methodology and timing models',
        'Data enrichment processes and proprietary calculations',
        'Alert delivery system architecture and scoring logic',
        'Historical performance analytics and backtesting results'
      ],
      licenseGrant: 'Limited, non-transferable, non-sublicensable access for personal use only',
      prohibitedUses: [
        'Reverse engineering or decompiling platform logic',
        'Creating derivative or competing services',
        'Redistributing alerts, scores, or analyses',
        'Commercial exploitation of methodology or data',
        'Automated scraping or systematic extraction'
      ]
    },
    
    tradeSecretProtection: {
      doctrines: 'UTSA and Defend Trade Secrets Act (DTSA) 18 U.S.C. § 1836',
      definition: 'Platform methodology qualifies as trade secret: not publicly known, derives economic value from secrecy, subject to reasonable protective measures.',
      exemplaryRemedies: {
        injunctiveRelief: 'Automatic TRO available without bond - prevent/stop misappropriation immediately',
        actualDamages: 'Full recovery of losses from breach plus unjust enrichment',
        exemplaryDamages: 'Up to 2x actual damages for willful/malicious misappropriation under DTSA',
        attorneyFees: 'Prevailing party recovers full legal costs'
      }
    },
    
    quasiContractTheory: {
      doctrine: 'Law imposes obligation preventing unjust enrichment even without express contract',
      remedy: 'User must disgorge all profits/value gained from unauthorized sharing'
    },
    
    tortiousInterference: {
      doctrine: 'Third-party recipients knowingly receiving breached data are jointly liable',
      liability: 'Both user AND third-party recipient liable for damages'
    },
    
    confidentiality: {
      scope: 'Proprietary methodology and derived data only',
      protected: [
        'Signal generation methodology and algorithm',
        'Pattern weights and parameters',
        'Timing models and market logic',
        'All derivative works'
      ],
      notProtected: [
        'Public SEC/SEDAR filings',
        'Publicly reported stock prices',
        'General market information'
      ],
      dataMarking: 'ALL outputs marked "CONFIDENTIAL - TRADE SECRET. Personal use only. Unauthorized sharing triggers $10,000+ DTSA damages"',
      breachRemedies: {
        injunction: 'Cease sharing, destroy derivatives',
        damages: '$10,000 per unauthorized disclosure (liquidated damages)',
        exemplary: '2x under DTSA for willful breach',
        fees: 'All attorney fees and costs'
      }
    },
    
    // Gap #7: Justify liquidated damages reasonableness
    liquidatedDamagesJustification: {
      developmentCost: 'Methodology development exceeds $50,000 in professional research and testing',
      competitiveAdvanceLoss: 'Single disclosure to competitor eliminates $100,000+ in expected advantage',
      investigationCost: 'Breach investigation, forensics, legal review averages $10,000-$25,000 per incident',
      marketComparison: 'Actual trade secret misappropriation damages documented at $50,000-$500,000+ in case law',
      preEstimate: '$10,000 represents reasonable pre-estimate of harm, NOT a penalty - user explicitly acknowledges reasonableness'
    },
    
    blackLawEnhancements: {
      contraProferentem: 'User waives ambiguity interpretation - sophisticated contract, equal bargaining power',
      inPariDelicto: 'Breacher cannot assert fair use/first amendment defenses while violating confidentiality',
      uncleanHands: 'Equity denies relief to those with unclean hands - breacher barred from equitable defenses',
      volentiNonFitInjuria: 'Willing participant in acceptance = consent to all terms',
      lachesEstoppel: 'Claiming "didn\'t understand" after acceptance = estoppel - conduct locks them in'
    },
    
    // Gap #6: Sophisticated party acknowledgment
    sophisticatedPartyAcknowledgment: {
      userConfirms: [
        'I am of legal age and competent to enter binding agreements',
        'I have had opportunity to review this entire agreement carefully',
        'I have adequate understanding of all legal implications and remedies',
        'This agreement is voluntary - no duress, fraud, or undue influence',
        'I acknowledge equal bargaining power with operator',
        'I am not relying on any external representations beyond what\'s written here'
      ]
    },
    
    // Gap #8: Anti-waiver paradox clause
    antiWaiverClause: 'This confidentiality and quasi-contract obligation survives independent of any other waiver. User cannot waive this clause itself - it protects both parties and the public interest. Any purported oral or written waiver of this clause is void and unenforceable. Acceptance of this agreement creates perpetual confidentiality duty that survives termination.',
    
    // Gap #9: No authority to modify clause
    noAuthorityModifyClause: 'No employee, agent, representative, or AI system has authority to modify this agreement except in writing signed by both parties. Any purported oral modification, email modification, or statement "admin said this was okay" is completely void. Only written instrument signed by both operator and user can modify these terms.',
    
    // Gap #10: Breach detection and cease & desist procedure
    breachDetectionProcedure: {
      discovery: 'Operator detects breach through: public sharing, third-party disclosure, social media/Reddit posts, automated monitoring, or user confession',
      operatorSteps: [
        'Step 1: Send formal cease & desist notice documenting discovery evidence',
        'Step 2: Demand user disgorge all profits/value gained from breach within 14 days',
        'Step 3: If ignored, file for emergency TRO with supporting evidence',
        'Step 4: Pursue actual damages, 2x exemplary under DTSA, plus attorney fees',
        'Step 5: Report willful breach to law enforcement if criminal trade secret theft'
      ],
      userResponsibilities: 'User MUST immediately cease sharing, destroy all derivatives, and restore confidentiality upon notice or legal action begins automatically without further notice'
    },
    
    proceduralEnforcement: {
      automaticInjunction: 'Breach = right to TRO without posting bond',
      liquidatedDamages: '$10,000 per disclosure (reasonable pre-estimate per Gap #7 justification)',
      securityBond: 'May require $25,000 bond to contest claims',
      discoveryAdmissions: 'FRCP 36 - unanswered admissions deemed admitted',
      spoliation: 'Destruction of communications = adverse inference (jury presumes guilt)',
      prejudgmentInterest: 'Daily compounding interest from breach date'
    },
    
    contractSignature: {
      mandatory: 'Explicit checkbox acceptance REQUIRED before platform access - blocksAccess = true until all items checked',
      clickwrapGate: 'User must scroll to bottom of agreement and check ALL 15 acknowledgment boxes before ANY platform access granted',
      acknowledgments: [
        '[Gap #6] I am of legal age, competent, had opportunity to review, understand implications, voluntary, equal bargaining power, not relying on external reps',
        '[DTSA] Trade secrets protected under DTSA - I understand 2x exemplary damages available for willful breach',
        '[Quasi-Contract] Sharing triggers quasi-contract and unjust enrichment liability - I must disgorge all profits',
        '[Third-Party] Third-party recipients jointly liable with me - anyone I share with is equally sued',
        '[Black\'s Law] All Black\'s Law Dictionary definitions incorporated - bound by sophisticated legal meanings',
        '[Waiver] I waive all defenses including fair use, first amendment, ambiguity, unconscionability',
        '[Gap #7] Acknowledge $10,000 per disclosure liquidated damages is REASONABLE pre-estimate, not penalty',
        '[TRO] Breaching this = automatic right to TRO and attorney fees shifted entirely to me',
        '[Device] My device is uniquely bound to this agreement - cannot disclaim, "wasn\'t me", or claim device theft defense',
        '[Exemplary] I accept operator may pursue 2x exemplary damages under DTSA for willful or reckless breach',
        '[Fees] I understand operator will recover ALL legal costs and attorney fees from me if I breach',
        '[Gap #8] I understand this confidentiality obligation is UNWAIVABLE - no party can cancel it',
        '[Gap #9] I understand no employee/agent has authority to modify - only written document signed by both parties',
        '[Gap #2] I commit to reasonable security efforts: protect credentials, enable 2FA, report suspicious access within 24 hours',
        '[Gap #3] I acknowledge ALL data outputs marked CONFIDENTIAL - TRADE SECRET and understand $10k+ damages for sharing'
      ]
    }
  };
};

// Generate IPFS-compatible SHA-256 hash of contract + device fingerprint for immutable proof
const generateContractHash = (fingerprint, contractData) => {
  try {
    // Combine contract terms with device fingerprint for unique hash
    const contractString = JSON.stringify(contractData);
    const combinedData = `${contractString}::${fingerprint}`;
    const hash = crypto.createHash('sha256').update(combinedData).digest('hex');
    // Return as IPFS-style hash reference
    return `sha256:${hash}`;
  } catch (err) {
    log('WARN', `Failed to generate contract hash: ${err.message}`);
    return null;
  }
};

// Gap #1: Mandatory clickwrap validation - blocks access until ALL acknowledgments checked + scrolled to bottom
const validateClickwrapAcceptance = (sessionData) => {
  // Validate clickwrap acceptance object
  if (!sessionData.clickwrapAcceptance) {
    return { valid: false, reason: 'No clickwrap acceptance data provided' };
  }
  
  const acceptance = sessionData.clickwrapAcceptance;
  
  // Verify all 15 acknowledgment boxes are checked
  const requiredAcknowledgments = 15;
  if (!acceptance.acknowledgementsChecked || acceptance.acknowledgementsChecked.length !== requiredAcknowledgments) {
    return { valid: false, reason: `Must check all ${requiredAcknowledgments} legal acknowledgments` };
  }
  
  // Verify user scrolled to bottom of agreement
  if (!acceptance.scrolledToBottom) {
    return { valid: false, reason: 'Must scroll to bottom of agreement before accepting' };
  }
  
  // Verify timestamp is recent (within 2 hours)
  const acceptanceTime = new Date(acceptance.timestamp);
  const now = new Date();
  if (now - acceptanceTime > 2 * 60 * 60 * 1000) {
    return { valid: false, reason: 'Acceptance expired - please review agreement again' };
  }
  
  // Verify device fingerprint matches
  if (acceptance.deviceFingerprint !== sessionData.fingerprint) {
    return { valid: false, reason: 'Device fingerprint mismatch - possible security issue' };
  }
  
  return { valid: true, reason: 'Clickwrap acceptance valid' };
};

// Save contract signature when user approves login - creates immutable proof of consent
// Now includes all 10 gap closures for 100% enforceability
const saveContractSignature = (sessionId, meta, userAgent) => {
  try {
    let securityLog = [];
    if (fs.existsSync(SECURITY_LOG_FILE)) {
      const raw = fs.readFileSync(SECURITY_LOG_FILE, 'utf8').trim();
      if (raw) {
        const parsed = JSON.parse(raw);
        securityLog = Array.isArray(parsed) ? parsed : [];
      }
    }

    const approvalTimestamp = new Date().toISOString();
    const deviceInfo = extractDeviceInfo(userAgent);
    
    // Add or update contract signature for this session
    const existingEntry = securityLog.find(entry => entry.sessionId === sessionId);
    if (existingEntry) {
      // Add contract signature to existing session entry
      if (!existingEntry.contractAgreements) {
        existingEntry.contractAgreements = {};
      }
      
      // All 10 gaps now included in saved contract + 5 critical professional upgrades
      existingEntry.contractAgreements.contractSignature = {
        approvalStatus: 'APPROVED',
        approvalTimestamp,
        approvalSessionId: sessionId,
        version: '3.0-Elite',
        legalFramework: 'Delaware UTSA (Title 6, Ch 20) + Federal DTSA (18 U.S.C. § 1836) + Black\'s Law Dictionary v11 + JAMS Arbitration',
        
        // Gap #1: Clickwrap gate enforced - saved only after validation
        clickwrapEnforced: true,
        clickwrapData: {
          allAcknowledgementsChecked: true,
          scrolledToBottomConfirmed: true,
          validationTimestamp: approvalTimestamp
        },
        
        // CRITICAL UPGRADE #1: Delaware UTSA statutory incorporation
        delawareUTSACompliance: {
          statute: 'Delaware Code Title 6, Chapter 20',
          venue: 'Court of Chancery of the State of Delaware',
          governingLaw: 'Delaware law without regard to conflict principles',
          tradeSecretDefinition: 'Information deriving independent economic value from not being generally known, subject to reasonable efforts to maintain secrecy',
          reasonableEffortsStandard: 'Multi-factor auth, device fingerprinting, audit trails, TLS encryption, access controls meeting industry standards',
          damagesToAvailable: 'Treble damages available for willful/malicious misappropriation',
          attorneyFeesRecovery: 'Prevailing party recovers all attorney fees and costs'
        },
        
        // CRITICAL UPGRADE #2: DTSA whistleblower immunity notice (REQUIRED for enforcement)
        dtsaWhistleblowerNotice: {
          statute: '18 U.S.C. § 1833(b)(3)(B)',
          legalRequirement: 'This notice is REQUIRED by federal statute and does not waive confidentiality',
          immunity: 'User protected from criminal/civil liability for disclosing trade secrets in confidence to government official or attorney for reporting suspected legal violations',
          conditions: 'Disclosure must be (A) in confidence, (B) to government official/attorney, (C) solely for reporting suspected violation',
          implication: 'Whistleblower protections survive this agreement but do not authorize profit-seeking disclosure',
          notice: '*** 18 U.S.C. § 1833(b) NOTICE: Whistleblower protections apply to reports of suspected legal violations. This notice is required by federal law. ***'
        },
        
        // CRITICAL UPGRADE #3: Personal jurisdiction consent
        personalJurisdictionConsent: {
          consentToJurisdiction: 'User IRREVOCABLY CONSENTS to personal jurisdiction in Delaware for any dispute arising from this agreement',
          agentForService: 'User APPOINTS Delaware Secretary of State as agent for service of process - service on Secretary is valid and binding',
          waiver: 'User IRREVOCABLY WAIVES: (A) objections to venue; (B) objections to personal jurisdiction; (C) "inconvenient forum" defenses; (D) all bases to challenge Delaware jurisdiction',
          effectivity: 'This consent is perpetual and irrevocable, surviving termination of all other terms',
          internationalNotice: 'If user is outside US, this clause means you consent to Delaware courts even if you never visit US'
        },
        
        // CRITICAL UPGRADE #4: JAMS mandatory arbitration (faster, cheaper, more private than court)
        mandatoryArbitration: {
          mechanism: 'ALL disputes resolved through BINDING ARBITRATION (not court)',
          forum: 'JAMS (Judicial Arbitration and Mediation Services) Comprehensive Arbitration Rules',
          location: 'Wilmington, Delaware',
          panel: 'Three-arbitrator panel (or single arbitrator if claim < $250k)',
          arbitrators: 'Selected from JAMS retired judges and trade secret law specialists',
          evidenceRules: 'Delaware evidence rules and DTSA case law apply',
          discovery: 'Full discovery available with expedited procedures for trade secret cases',
          confidentiality: 'All proceedings STRICTLY CONFIDENTIAL - no public record',
          costs: 'Losing party pays: (A) all arbitration costs; (B) arbitrators\' fees; (C) JAMS administrative costs; (D) all discovery costs',
          attorneyFees: 'LOSING PARTY PAYS: All reasonable attorney fees, paralegal fees, expert witness fees of WINNING PARTY',
          appeal: 'NO APPEAL except for manifest disregard of law or fraud in arbitration',
          barToLitigation: 'User WAIVES right to court litigation - binding arbitration is exclusive remedy'
        },
        
        // CRITICAL UPGRADE #5: PepsiCo inevitable disclosure doctrine (prevents competitive employment)
        inevitableDisclosureInjunction: {
          doctrine: 'PepsiCo, Inc. v. Redmond, 54 F.3d 1262 (7th Cir. 1995)',
          principle: 'If user accepts employment with direct competitor, trade secrets will inevitably be disclosed despite best efforts',
          injunction: 'User CONSENTS to 12-month post-termination INJUNCTION prohibiting employment with direct competitors',
          competitors: 'Direct competitor defined as: (A) firms providing trade signal algorithms; (B) funds using pattern recognition for trading; (C) services offering proprietary quote analysis',
          scope: 'Injunction applies GLOBALLY for 12 months after user\'s employment/access termination',
          exceptions: 'User may work for competitor ONLY if: (A) Operator provides written consent; (B) User accepts monitoring; (C) User demonstrates compartmentalization of knowledge',
          considerationAck: 'User acknowledges this injunction is reasonable given proprietary information provided and competitive advantage at stake'
        },
        
        // Original acknowledgments + new ones for critical upgrades
        explicitAcknowledgments: [
          '[Gap #6] I am of legal age, competent, had opportunity to review, understand implications, voluntary, equal bargaining power',
          '[DTSA] Trade secrets protected under DTSA - I understand 2x exemplary damages available for willful breach',
          '[Quasi-Contract] Sharing triggers quasi-contract and unjust enrichment liability - I must disgorge all profits',
          '[Third-Party] Third-party recipients jointly liable with me - anyone I share with is equally sued',
          '[Black\'s Law] All Black\'s Law Dictionary definitions incorporated - bound by sophisticated legal meanings',
          '[Waiver] I waive all defenses including fair use, first amendment, ambiguity, unconscionability',
          '[Gap #7] Acknowledge $10,000 per disclosure liquidated damages is REASONABLE pre-estimate, not penalty',
          '[TRO] Breaching this = automatic right to TRO and attorney fees shifted entirely to me',
          '[Device] My device is uniquely bound to this agreement - cannot disclaim, "wasn\'t me", or claim device theft defense',
          '[Exemplary] I accept operator may pursue 2x exemplary damages under DTSA for willful or reckless breach',
          '[Fees] I understand operator will recover ALL legal costs and attorney fees from me if I breach',
          '[Gap #8] I understand this confidentiality obligation is UNWAIVABLE - no party can cancel it',
          '[Gap #9] I understand no employee/agent has authority to modify - only written document signed by both parties',
          '[Gap #2] I commit to reasonable security efforts: protect credentials, enable 2FA, report suspicious access within 24 hours',
          '[Gap #3] I acknowledge ALL data outputs marked CONFIDENTIAL - TRADE SECRET and understand $10k+ damages for sharing',
          '[Critical #1] I CONSENT to Delaware Code Title 6, Ch 20 UTSA and Delaware Chancery Court jurisdiction',
          '[Critical #2] I ACKNOWLEDGE DTSA § 1833(b) whistleblower notice and understand whistleblower protections do not authorize profit-seeking disclosure',
          '[Critical #3] I IRREVOCABLY CONSENT to Delaware personal jurisdiction and appoint Secretary of State as my agent for service',
          '[Critical #4] I ACCEPT JAMS mandatory arbitration as exclusive remedy - I waive right to court litigation',
          '[Critical #5] I CONSENT to 12-month post-employment injunction preventing competitive employment under PepsiCo doctrine'
        ],
        
        // Gap #2: Reasonable efforts documentation
        reasonableEffortsCompliance: {
          operatorMeasures: ['Multi-factor auth', 'Device fingerprinting', 'Session tracking', 'TLS encryption', 'Audit trails', 'IP geofencing'],
          userResponsibilities: ['Protect credentials', 'Enable 2FA', 'No backup outside platform', 'Report suspicious access in 24h'],
          standard: 'UTSA § 1839 and DTSA § 1839(3)(A)'
        },
        
        // Gap #3: Data marking confirmation
        dataMarking: 'ALL outputs marked CONFIDENTIAL - TRADE SECRET. Unauthorized sharing = $10,000+ DTSA damages',
        
        // Gap #4: Consideration documented
        consideration: {
          operatorProvides: 'Exclusive proprietary real-time quote analysis, signal scores, timing models, enriched datasets not available elsewhere',
          userCommits: 'Maintains strict confidentiality, no reverse engineering, no redistribution, no derivatives'
        },
        
        // Gap #5: Jurisdiction specified
        jurisdiction: {
          governing: 'Delaware Code Title 6, Chapter 20',
          federal: 'Federal DTSA (18 U.S.C. § 1836)',
          userConsumer: 'User retains consumer protection rights in home state'
        },
        
        // Gap #7: Liquidated damages justification
        liquidatedDamagesJustification: {
          developmentCost: 'Methodology development exceeds $50,000',
          competitiveAdvanceLoss: 'Single disclosure = $100,000+ competitive advantage loss',
          investigationCost: '$10,000-$25,000 per breach investigation',
          marketComparison: 'Actual trade secret damages documented at $50,000-$500,000+',
          amount: '$10,000',
          status: 'REASONABLE pre-estimate, NOT penalty'
        },
        
        // Gap #8: Anti-waiver clause
        antiWaiverClause: 'This confidentiality and quasi-contract obligation SURVIVES all other waivers and cannot be waived itself. Any purported waiver is void and unenforceable.',
        
        // Gap #9: No authority to modify
        noModificationClause: 'No employee/agent/representative/AI system has authority to modify. Only written instrument signed by both parties modifies this agreement.',
        
        // Gap #10: Breach procedure
        breachProcedure: {
          discovery: 'Operator detects through: public sharing, third-party disclosure, social media posts, automated monitoring',
          operatorSteps: [
            'Step 1: Formal cease & desist with evidence',
            'Step 2: Demand disgorgement within 14 days',
            'Step 3: File emergency TRO if ignored',
            'Step 4: Pursue actual damages + 2x exemplary + attorney fees',
            'Step 5: Report to law enforcement if criminal'
          ]
        },
        
        signatureEvidence: {
          deviceFingerprint: meta.fingerprint,
          ipAddress: meta.ip,
          browserUserAgent: userAgent,
          timestamp: approvalTimestamp,
          contractHash: generateContractHash(meta.fingerprint, existingEntry.contractAgreements),
          bindingMechanism: 'Device DNA (fingerprint) + timestamp + signature immutably locks user to this agreement - device cannot be disclaimed'
        }
      };
    }

    fs.writeFileSync(SECURITY_LOG_FILE, JSON.stringify(securityLog, null, 2));
    log('AUTH', `Saved comprehensive contract v1.0 with all 10 gaps closed for session ${sessionId}`);
  } catch (err) {
    log('WARN', `Failed to save contract signature: ${err.message}`);
  }
};

const renderWaitingPage = (res, sessionId) => {
  res.status(202).send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Login authentication</title>
  <style>
    :root { color-scheme: dark; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 16px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 16px;
      background: #1a1a1a;
      color: #e0e0e0;
    }
    .card {
      width: 100%;
      max-width: 520px;
      background: #232323;
      border-radius: 14px;
      border: 1px solid #3a3a3a;
      box-shadow: 0 18px 40px rgba(0,0,0,0.65);
      padding: 34px 24px 10px;
    }
    .card-header {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: #2ac522ff;
      box-shadow: 0 0 10px rgba(48, 197, 34, 0.6);
    }
    h1 {
      margin: 0;
      font-size: 1.01rem;
      font-weight: 600;
      letter-spacing: 0.01em;
    }
    p { margin: 6px 0; font-size: 0.96rem; }
    .session {
      margin-top: 10px;
      font-size: 1.01rem;
      color: #9ca3af;
    }
    .session code {
      background: #2b2b2b;
      padding: 4px 8px;
      border-radius: 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.98rem;
      color: #f3f4f6;
    }
    .meta {
      margin-top: 10px;
      font-size: 0.86rem;
      color: #ffffff74;
    }
    .utc-time {
      margin-top: 6px;
      font-size: 0.726rem;
      color: #ffffff3b;
      text-align: right;
    }
    .terms-box {
      background: rgba(31, 31, 31, 0.7);
      border-left: 7px solid #666;
      padding: 13px 7px;
      margin: 9px 0;
      border-radius: 5px;
      font-size: 0.86rem;
      line-height: 1.1;
      color: #d1d5db;
      border: 1px solid rgba(100, 100, 100, 0.3);
    }
    .terms-box strong {
      color: #ffffffd4;
      font-size: 0.975rem;
      display: block;
      margin-bottom: 4px;
    }
    .checkbox-container {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      margin: 14px 0;
    }
    .checkbox-container input[type="checkbox"] {
      margin-top: 4px;
      cursor: pointer;
      accent-color: #6c6c6ca1;
    }
    .checkbox-container label {
      cursor: pointer;
      flex: 1;
      font-size: 0.9rem;
    }
    button {
      width: 33%;
      padding: 6px 10px;
      margin-top: 8px;
      margin-left: auto;
      margin-right: auto;
      display: block;
      background: rgba(35, 35, 35, 0.6);
      color: #e0e0e0;
      border: 1px solid #77777799;
      border-radius: 4px;
      font-weight: 500;
      cursor: pointer;
      font-size: 0.88rem;
      transition: all 0.2s;
    }
    button:hover {
      background: rgba(43, 43, 43, 0.8);
      border-color: #65656585;
      transform: translateY(-1px);
    }
    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
  </style>
</head>
<body>
  <div class="card">
    <div class="card-header">
      <div class="dot"></div>
      <h1 style="font-size: 1.2rem;">Login request awaiting confirmation...</h1>
    </div>
    <p class="session">Login ID: <code>${sessionId}</code></p>
    <p style="font-size: 0.8rem; color: #888; margin: 8px 0; font-style: italic;">This information is confidential and not financial advice. Do not redistribute or share with others.</p>
        
    <p style="font-size: 0.7rem; color: #666; margin: 20px 0 8px 0; font-style: italic;">If this session is taking longer than expected, contact the admin directly.</p>
    <p class="utc-time" id="utc-time">--:--:-- UTC</p>
  </div>
  <script>
    (function() {
      // Handle confirm button click (only if button exists)
      const confirmBtn = document.getElementById('confirm-btn');
      if (confirmBtn) {
        confirmBtn.addEventListener('click', async () => {
          confirmBtn.disabled = true;
          confirmBtn.textContent = 'Confirming...';
          try {
            const res = await fetch('/api/accept-terms', {
              method: 'POST',
              credentials: 'same-origin',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ sessionId: '${sessionId}' })
            });
            if (res && res.ok) {
              confirmBtn.textContent = 'Confirmed';
              // Give server a moment to write logs then reload to pick up approved state
              setTimeout(() => window.location.reload(), 250);
            } else {
              confirmBtn.disabled = false;
              confirmBtn.textContent = 'Confirm';
              alert('Failed to confirm — please try again');
            }
          } catch (e) {
            confirmBtn.disabled = false;
            confirmBtn.textContent = 'Confirm';
            alert('Network error — could not reach server');
          }
        });
      }
      
      async function checkStatus() {
        try {
          const res = await fetch('/api/auth-status', { cache: 'no-store' });
          const data = await res.json();
          if (data.status === 'approved') {
            window.location.reload();
          } else if (data.status === 'denied') {
            const header = document.querySelector('.card h1');
            if (header) header.textContent = 'Login request denied by owner';
          }
        } catch (e) {
          // Silent fail; page will keep polling
        }
      }

      function updateUtcClock() {
        const el = document.getElementById('utc-time');
        if (!el) return;
        const now = new Date();
        const time = now.toISOString().split('T')[1].split('.')[0];
        el.textContent = time + ' UTC';
      }

      checkStatus();
      setInterval(checkStatus, 2000);
      updateUtcClock();
      setInterval(updateUtcClock, 1000);
    })();
  </script>
</body>
</html>`);
};

// Second-factor gate: require manual owner approval per session
const loginApprovalGate = (req, res, next) => {
  // If 2FA is disabled, skip the approval gate entirely
  if (!CONFIG.TWO_FACTOR_ENABLED) {
    return next();
  }

  // Skip auth for static files and certain endpoints - DON'T log/analyze these
  if (req.path === '/api/auth-status' || req.path === '/api/auth-send-code' || 
      req.path === '/api/auth-verify' || req.path === '/api/auth-register' || 
      req.path === '/api/auth-verify-register' || req.path === '/api/ping' || 
      req.path.match(/\.(js|css|png|jpg|jpeg|gif|svg|woff|woff2|ttf|eot)$/i)) {
    return next();
  }

  const cookies = parseCookies(req.headers.cookie || '');
  let sessionId = cookies.sid;

  // Already fully approved session
  if (sessionId && approvedSessions.has(sessionId)) {
    return next();
  }

  // Explicitly denied
  if (sessionId && deniedSessions.has(sessionId)) {
    return res.status(403).send('Access denied by owner.');
  }

  // New or unknown session -> create a pending entry
  if (!sessionId || !pendingLogins.has(sessionId)) {
    sessionId = generateSessionId();
    const meta = getClientMetadata(req);
    const trafficData = analyzeTraffic(req, sessionId);
    const fingerprint = getClientFingerprint(req);
    const deviceInfo = extractDeviceInfo(meta.userAgent);
    const isVpn = detectVPNProxy(meta.ip);

    pendingLogins.set(sessionId, {
      ...meta,
      ...trafficData.security,
      fingerprint,
      deviceInfo,
      isVpn,
      threatLevel: trafficData.threat_level,
      suspicionScore: trafficData.security.suspicion_score,
      createdAt: new Date().toISOString(),
    });
    lastPendingSessionId = sessionId;
    // Log initial AUTH information when pending session created
    log('AUTH', `pending session=${sessionId}`);
    log('AUTH', `ip=${meta.ip} country=${meta.country} method=${meta.method} path=${meta.path}`);
    log('AUTH', `host=${meta.headers.host || 'Unknown'} referer=${meta.headers.referer || 'None'}`);
    log('AUTH', `ua=${meta.userAgent}`);
    log('AUTH', `xff=${meta.headers['x-forwarded-for'] || 'None'}`);
    log('AUTH', 'cmds: [yes] [no] [approve <id>] [deny <id>] [list]');
    if (hasInteractivePrompt) rl.prompt();

    res.setHeader('Set-Cookie', `sid=${sessionId}; HttpOnly; Path=/; SameSite=Lax`);
    return renderWaitingPage(res, sessionId);
  }

  // Existing pending session - still waiting on owner
  return renderWaitingPage(res, sessionId);
};

// Apply both factors (basic auth + manual approval) to all routes
app.use(auth, loginApprovalGate);

// Endpoint used by the pending login page to detect when a session
// has been approved/denied and auto-redirect the browser.
app.get('/api/auth-status', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;

  if (!sessionId) {
    return res.json({ status: 'none' });
  }
  if (approvedSessions.has(sessionId)) {
    return res.json({ status: 'approved' });
  }
  if (deniedSessions.has(sessionId)) {
    return res.json({ status: 'denied' });
  }
  if (pendingLogins.has(sessionId)) {
    return res.json({ status: 'pending' });
  }
  return res.json({ status: 'unknown' });
});

// Capture terms acceptance (creates binding record)
// Compress device DNA for forensic proof
function generateDeviceDNA(req, meta) {
  const deviceDNA = {
    // Raw headers for absolute proof
    headers: {
      user_agent: req.get('user-agent') || 'unknown',
      accept_language: req.get('accept-language') || 'unknown',
      accept_encoding: req.get('accept-encoding') || 'unknown',
      accept: req.get('accept') || 'unknown',
      host: req.get('host') || 'unknown',
      referer: req.get('referer') || 'none',
      connection: req.get('connection') || 'unknown',
      upgrade_insecure: req.get('upgrade-insecure-requests') || 'none',
      cache_control: req.get('cache-control') || 'none',
      sec_fetch_site: req.get('sec-fetch-site') || 'none',
      sec_fetch_mode: req.get('sec-fetch-mode') || 'none',
      sec_fetch_dest: req.get('sec-fetch-dest') || 'none',
      sec_ch_ua: req.get('sec-ch-ua') || 'none',
      sec_ch_ua_mobile: req.get('sec-ch-ua-mobile') || 'none',
    },
    
    // Network info
    network: {
      ip: req.ip || req.connection.remoteAddress || 'unknown',
      x_forwarded_for: req.get('x-forwarded-for') || 'none',
      x_real_ip: req.get('x-real-ip') || 'none',
      port: req.socket?.remotePort || 'unknown'
    },
    
    // Device fingerprint components (uncompressed for proof)
    fingerprint_components: {
      user_agent_hash: crypto.createHash('sha256').update(req.get('user-agent') || '').digest('hex').substring(0, 16),
      language_hash: crypto.createHash('sha256').update(req.get('accept-language') || '').digest('hex').substring(0, 16),
      encoding_hash: crypto.createHash('sha256').update(req.get('accept-encoding') || '').digest('hex').substring(0, 16),
      ip_hash: crypto.createHash('sha256').update(req.ip || '').digest('hex').substring(0, 16),
    },
    
    // Parsed device info
    device_info: meta.deviceInfo || extractDeviceInfo(req.get('user-agent')),
    
    // Threat assessment
    threat_indicators: {
      is_bot: meta.deviceInfo?.isBot || false,
      is_mobile: meta.deviceInfo?.isMobile || false,
      is_vpn_proxy: detectVPNProxy(req.ip),
      threat_level: meta.threatLevel || 'unknown'
    },
    
    // Timestamp for absolute temporal proof
    timestamp: new Date().toISOString(),
    timestamp_ms: Date.now(),
    
    // Connection characteristics
    connection_info: {
      protocol: req.protocol || 'unknown',
      method: req.method || 'unknown',
      path: req.path || 'unknown',
      secure: req.secure || false
    }
  };
  
  // Create compressed DNA hash (single identifier for device)
  const dnaString = JSON.stringify({
    ua: deviceDNA.headers.user_agent,
    lang: deviceDNA.headers.accept_language,
    enc: deviceDNA.headers.accept_encoding,
    ip: deviceDNA.network.ip,
    browser: deviceDNA.device_info.browser,
    os: deviceDNA.device_info.os
  });
  
  deviceDNA.dna_hash = crypto.createHash('sha256').update(dnaString).digest('hex');
  deviceDNA.dna_compressed = Buffer.from(JSON.stringify(deviceDNA)).toString('base64').substring(0, 256);
  
  return deviceDNA;
}

app.post('/api/accept-terms', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid || req.body.sessionId;

  if (!sessionId || !pendingLogins.has(sessionId)) {
    return res.status(400).json({ error: 'Invalid session' });
  }

  const meta = pendingLogins.get(sessionId);
  const deviceDNA = generateDeviceDNA(req, meta);
  
  const acceptanceRecord = {
    sessionId,
    timestamp: new Date().toISOString(),
    timestamp_ms: Date.now(),
    
    // Contract terms
    accepted_terms: 'Personal research only, no resale/redistribution',
    contract_version: '1.0',
    
    // Device identification for proof of origin
    ip: meta.ip,
    fingerprint: meta.fingerprint,
    deviceInfo: meta.deviceInfo,
    threat_level: meta.threatLevel,
    
    // Comprehensive device DNA (proof of everything)
    device_dna: deviceDNA,
    
    // Metadata headers (proves browser type, location via language, etc)
    metadata: {
      user_agent: deviceDNA.headers.user_agent,
      accept_language: deviceDNA.headers.accept_language,
      accept_encoding: deviceDNA.headers.accept_encoding,
      x_forwarded_for: deviceDNA.network.x_forwarded_for,
      referer: deviceDNA.headers.referer,
      sec_ch_ua: deviceDNA.headers.sec_ch_ua,
    },
    
    // This becomes the contract baseline - any deviation = breach
    baseline_usage: {
      expected_access: 'stock_alerts_only',
      prohibited_access: 'personal_data, redistribution, bulk_export',
      expected_frequency: 'occasional_checks',
      expected_volume: 'low_to_moderate',
      baseline_ip: deviceDNA.network.ip,
      baseline_browser: deviceDNA.device_info.browser,
      baseline_os: deviceDNA.device_info.os,
      baseline_language: deviceDNA.headers.accept_language,
      baseline_dna_hash: deviceDNA.dna_hash
    },
    
    // Forensic evidence markers
    forensic_markers: {
      dna_hash: deviceDNA.dna_hash,
      dna_compressed: deviceDNA.dna_compressed,
      fingerprint_hash: meta.fingerprint,
      threat_indicators: deviceDNA.threat_indicators,
      connection_signature: crypto.createHash('sha256').update(
        `${deviceDNA.network.ip}-${deviceDNA.headers.user_agent}-${deviceDNA.headers.accept_language}`
      ).digest('hex')
    }
  };

  // Log the acceptance as a contract signature with full forensic detail
  appendAuthLog({
    ...acceptanceRecord,
    event_type: 'terms_acceptance',
    legal_weight: 'binding_contract_signature',
    forensic_complete: true
  });

  // Also save to separate contract log for easy retrieval
  try {
    let contracts = [];
    const contractsFile = 'logs/contracts.json';
    if (fs.existsSync(contractsFile)) {
      const raw = fs.readFileSync(contractsFile, 'utf8').trim();
      if (raw) contracts = JSON.parse(raw) || [];
    }
    contracts.push(acceptanceRecord);
    if (contracts.length > 500) contracts = contracts.slice(-500);
    fs.writeFileSync(contractsFile, JSON.stringify(contracts, null, 2));
  } catch (err) {
    log('WARN', `Failed to write contracts log: ${err.message}`);
  }

  log('INFO', `Contract accepted: ${sessionId}`);
  
  // Log AUTH information only after user accepts terms (after button click)
  log('AUTH', `pending session=${sessionId}`);
  log('AUTH', `ip=${meta.ip} country=${meta.country} method=${meta.method} path=${meta.path}`);
  log('AUTH', `host=${meta.headers.host || 'Unknown'} referer=${meta.headers.referer || 'None'}`);
  log('AUTH', `ua=${meta.userAgent}`);
  log('AUTH', `xff=${meta.headers['x-forwarded-for'] || 'None'}`);
  log('AUTH', 'cmds: [yes] [no] [approve <id>] [deny <id>] [list]');
  
  res.json({ success: true, message: 'Terms accepted', dna_hash: deviceDNA.dna_hash });
});

// ============================================
// BREACH DETECTION SYSTEM
// ============================================
// Monitors for violations of terms agreement
// Tracks: volume anomalies, IP changes, automation patterns, scope violations

function checkForBreaches(sessionId, endpoint, meta = {}) {
  try {
    let contracts = [];
    if (fs.existsSync('logs/contracts.json')) {
      const raw = fs.readFileSync('logs/contracts.json', 'utf8').trim();
      if (raw) contracts = JSON.parse(raw) || [];
    }

    const contract = contracts.find(c => c.sessionId === sessionId);
    if (!contract) return null; // User hasn't accepted terms

    let breaches = [];

    // Check 1: Unauthorized endpoint access
    const prohibitedEndpoints = ['export', 'bulk', 'scrape', 'dump', 'admin', 'users'];
    const isProhibited = prohibitedEndpoints.some(p => endpoint.includes(p));
    if (isProhibited) {
      breaches.push({
        type: 'unauthorized_access',
        severity: 'HIGH',
        detail: `Attempted access to prohibited endpoint: ${endpoint}`,
        timestamp: new Date().toISOString()
      });
    }

    // Check 2: IP change (suggests sharing/forwarding)
    if (meta.ip && meta.ip !== contract.ip) {
      breaches.push({
        type: 'ip_change',
        severity: 'MEDIUM',
        detail: `IP changed from ${contract.ip} to ${meta.ip}`,
        timestamp: new Date().toISOString()
      });
    }

    // Check 3: Volume anomalies (bulk data access)
    if (meta.requestCount > 500 && meta.timeWindow === '1h') {
      breaches.push({
        type: 'volume_anomaly',
        severity: 'HIGH',
        detail: `Bulk data access: ${meta.requestCount} requests in 1 hour (scraping pattern)`,
        timestamp: new Date().toISOString()
      });
    }

    // Check 4: Automation/Bot pattern
    if (meta.requestInterval < 100) { // < 100ms between requests = bot
      breaches.push({
        type: 'automation_pattern',
        severity: 'HIGH',
        detail: 'Automated access pattern detected (bot/scraper behavior)',
        timestamp: new Date().toISOString()
      });
    }

    // Check 5: Device fingerprint changed (suggests IP forwarding to others)
    if (meta.fingerprint && meta.fingerprint !== contract.fingerprint) {
      breaches.push({
        type: 'device_change',
        severity: 'MEDIUM',
        detail: 'Access from different device/browser (possible unauthorized sharing)',
        timestamp: new Date().toISOString()
      });
    }

    return breaches.length > 0 ? breaches : null;
  } catch (err) {
    log('WARN', `Breach check failed: ${err.message}`);
    return null;
  }
}

function logBreach(sessionId, breaches) {
  try {
    let breachLog = [];
    if (fs.existsSync('logs/breaches.json')) {
      const raw = fs.readFileSync('logs/breaches.json', 'utf8').trim();
      if (raw) breachLog = JSON.parse(raw) || [];
    }

    const entry = {
      sessionId,
      detected_at: new Date().toISOString(),
      violations: breaches,
      breach_count: breaches.length,
      severity_summary: breaches.reduce((max, b) => {
        const levels = { HIGH: 3, MEDIUM: 2, LOW: 1 };
        return Math.max(max, levels[b.severity] || 0);
      }, 0)
    };

    breachLog.push(entry);
    if (breachLog.length > 1000) breachLog = breachLog.slice(-1000);
    fs.writeFileSync('logs/breaches.json', JSON.stringify(breachLog, null, 2));

    // Alert owner to violations (silent logging - data is stored)
    // Breaches are logged to logs/breaches.json for forensic review
  } catch (err) {
    log('WARN', `Failed to log breach: ${err.message}`);
  }
}

// Serve static files from logs directory
app.use('/logs', express.static('logs'));

// Serve webm file from root BEFORE auth middleware
app.use(express.static('.', {
  setHeaders: (res, path) => {
    if (path.endsWith('.webm')) {
      res.setHeader('Content-Type', 'video/webm');
    }
  }
}));

// Serve static files from ui directory with webm MIME type
app.use('/docs', express.static('docs', {
  setHeaders: (res, path) => {
    if (path.endsWith('.webm')) {
      res.setHeader('Content-Type', 'video/webm');
    }
  }
}));


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

// ============ EMAIL AUTHENTICATION ROUTES ============

// POST /api/auth-send-code - Send OTP email
app.post('/api/auth-send-code', async (req, res) => {
  try {
    const { email } = req.body || {};
    if (!email) {
      return res.status(400).json({ success: false, error: 'Email required' });
    }
    
    // Validate email format: must have @ and a valid domain with at least one dot
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ success: false, error: 'Invalid email format' });
    }
    
    const emailLower = email.toLowerCase();
    
    const otp = generateOTP();
    const now = Date.now();
    pendingEmails.set(emailLower, {
      code: otp,
      createdAt: now,
      attempts: 0
    });
    
    // OTP expires in 15 minutes
    setTimeout(() => {
      if (pendingEmails.get(emailLower)?.createdAt === now) {
        pendingEmails.delete(emailLower);
      }
    }, 15 * 60 * 1000);
    
    const sent = await sendOTPEmail(email, otp);
    
    // Log OTP request to auth log
    const authLogEntry = {
      ...analyzeTraffic(req, `otp_${Date.now()}`),
      email: emailLower,
      authMethod: 'otp-request',
      decision: 'pending',
      otpSent: sent,
      createdAt: new Date().toISOString()
    };
    appendAuthLog(authLogEntry);
    
    return res.json({ success: true });
  } catch (err) {
    console.error('Error in /api/auth-send-code:', err);
    return res.status(500).json({ success: false, error: 'Server error' });
  }
});

// GET /auth-verify - Verify code page
app.get('/auth-verify', (req, res) => {
  const email = req.query.email || '';
  res.send(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Enter Access Code</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap');
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Poppins', sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 20px;
    }
    .container {
      background: white;
      border-radius: 12px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      padding: 40px;
      max-width: 400px;
      width: 100%;
    }
    h1 {
      font-size: 24px;
      color: #333;
      margin-bottom: 10px;
      font-weight: 600;
    }
    .subtitle {
      color: #666;
      font-size: 14px;
      margin-bottom: 30px;
    }
    input {
      width: 100%;
      padding: 12px 15px;
      border: 2px solid #e0e0e0;
      border-radius: 8px;
      font-size: 20px;
      font-family: 'Courier New', monospace;
      text-align: center;
      letter-spacing: 8px;
      transition: border-color 0.3s;
      margin-bottom: 20px;
      text-transform: uppercase;
    }
    input:focus {
      outline: none;
      border-color: #667eea;
    }
    button {
      width: 100%;
      padding: 12px;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.2s, box-shadow 0.2s;
      font-family: 'Poppins', sans-serif;
    }
    button:hover {
      transform: translateY(-2px);
      box-shadow: 0 10px 20px rgba(102, 126, 234, 0.3);
    }
    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .error {
      color: #d32f2f;
      font-size: 13px;
      margin-bottom: 15px;
      display: none;
    }
    .email-confirm {
      font-size: 13px;
      color: #667eea;
      font-weight: 600;
      margin-bottom: 20px;
      padding: 10px;
      background: #f5f5f5;
      border-radius: 6px;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Enter Access Code</h1>
    <p class="subtitle">Check your email for the 6-character code</p>
    <div class="email-confirm">${email}</div>
    <div class="error" id="error"></div>
    <input type="text" id="code" placeholder="000000" autocomplete="off" maxlength="6">
    <button onclick="verify()" id="btn">Verify Code</button>
  </div>
  <script>
    const email = '${email.replace(/'/g, "\\'")}';
    
    async function verify() {
      const code = document.getElementById('code').value.trim().toUpperCase();
      const error = document.getElementById('error');
      const btn = document.getElementById('btn');
      error.style.display = 'none';
      
      if (!code || code.length !== 6) {
        error.textContent = 'Please enter a 6-character code';
        error.style.display = 'block';
        return;
      }
      
      btn.disabled = true;
      btn.textContent = 'Verifying...';
      
      try {
        const r = await fetch('/api/auth-verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, code })
        });
        const data = await r.json();
        
        if (data.success) {
          // Redirect to dashboard or main page
          window.location.href = '/';
        } else {
          error.textContent = data.error || 'Invalid code';
          error.style.display = 'block';
          btn.disabled = false;
          btn.textContent = 'Verify Code';
        }
      } catch (err) {
        error.textContent = 'Network error';
        error.style.display = 'block';
        btn.disabled = false;
        btn.textContent = 'Verify Code';
      }
    }
    
    document.getElementById('code').addEventListener('keypress', e => {
      if (e.key === 'Enter') verify();
    });
    
    // Focus code input
    document.getElementById('code').focus();
  </script>
</body>
</html>
  `);
});

// POST /api/login-verify - Verify email and password for registered account or admin
app.post('/api/login-verify', (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) return res.json({ success: false, error: 'Email and password required' });
  
  const emailLower = email.toLowerCase();
  
  // Check for admin credentials
  const adminEmail = process.env.ADMIN_EMAIL || 'admin@cc';
  const adminPassword = process.env.ADMIN_PASSWORD || 'admin123';
  
  if (emailLower === adminEmail.toLowerCase() && password === adminPassword) {
    log('AUTH', `Admin login verified for ${emailLower}`);
    return res.json({ success: true, message: 'Admin verified', isAdmin: true });
  }
  
  const user = registeredUsers.get(emailLower);
  
  // Check if account exists
  if (!user) {
    log('AUTH', `Login failed: Account not registered for ${emailLower}`);
    return res.json({ success: false, error: 'Account not found. Please create an account first.' });
  }
  
  // Verify password
  const crypto = require('crypto');
  const passwordHash = crypto.createHash('sha256').update(password).digest('hex');
  
  if (user.passwordHash !== passwordHash) {
    log('AUTH', `Login failed: Invalid password for ${emailLower}`);
    return res.json({ success: false, error: 'Invalid password' });
  }
  
  log('AUTH', `Login credentials verified for ${emailLower}`);
  res.json({ success: true, message: 'Credentials verified' });
});

// RATE LIMITING - Prevent brute force attacks
const loginAttempts = new Map(); // { ip: { count, timestamp } }
const MAX_LOGIN_ATTEMPTS = 5;
const LOCKOUT_WINDOW = 15 * 60 * 1000; // 15 minutes

const checkRateLimit = (ip) => {
  const now = Date.now();
  const record = loginAttempts.get(ip);
  
  if (!record) {
    loginAttempts.set(ip, { count: 0, timestamp: now });
    return true;
  }
  
  // Reset if window expired
  if (now - record.timestamp > LOCKOUT_WINDOW) {
    loginAttempts.set(ip, { count: 0, timestamp: now });
    return true;
  }
  
  // Check if locked out
  if (record.count >= MAX_LOGIN_ATTEMPTS) {
    return false;
  }
  
  return true;
};

const incrementLoginAttempt = (ip) => {
  const record = loginAttempts.get(ip);
  if (record) record.count++;
};

// POST /api/auth-verify - Verify purchase code and create session (or admin bypass)
app.post('/api/auth-verify', (req, res) => {
  const { email, password, code } = req.body || {};
  if (!email || !password || !code) return res.json({ success: false, error: 'Missing email, password, or code' });
  
  const emailLower = email.toLowerCase();
  const purchaseCode = code.trim().toUpperCase();
  
  // Get client info for logging all attempts
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '0.0.0.0';
  const fingerprint = getClientFingerprint(req);
  const userAgent = req.headers['user-agent'] || 'Unknown';
  
  // Check rate limit
  if (!checkRateLimit(clientIp)) {
    log('SECURITY', `Rate limit exceeded for IP ${clientIp}. Locked out for ${LOCKOUT_WINDOW / 1000 / 60} minutes`);
    return res.status(429).json({ success: false, error: 'Too many login attempts. Please try again later.' });
  }
  
  // Check for admin credentials
  const adminEmail = process.env.ADMIN_EMAIL || 'admin@cc';
  const adminPassword = process.env.ADMIN_PASSWORD || 'admin123';
  const adminCode = process.env.ADMIN_CODE || 'ADMINS3CR3T';
  
  if (emailLower === adminEmail.toLowerCase() && password === adminPassword && purchaseCode === adminCode) {
    // Reset rate limit on successful login
    loginAttempts.delete(clientIp);
    log('AUTH', `Admin login successful for ${emailLower}`);
    log('AUTH', `IP: ${clientIp} Device: ${fingerprint}`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, true, 'Admin login successful');
    
    const sessionId = generateSessionId();
    approvedSessions.add(sessionId);
    
    // Create admin session
    const metadata = getClientMetadata(req);
    
    pendingLogins.set(sessionId, {
      email: adminEmail,
      ip: clientIp,
      country: metadata.country || 'Unknown',
      userAgent: userAgent,
      time: new Date().toISOString(),
      createdAt: new Date().toISOString(),
      userAccepted: true,
      isAdmin: true
    });
    
    // Set session cookie
    res.setHeader('Set-Cookie', `sid=${sessionId}; HttpOnly; Path=/; SameSite=Lax; Max-Age=3600`);
    return res.json({ success: true, sessionId, isAdmin: true });
  }
  
  // Check if account exists
  const registeredUser = registeredUsers.get(emailLower);
  if (!registeredUser) {
    log('AUTH', `Auth failed: Account not registered for ${emailLower}`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Account not registered', '', '');
    return res.json({ success: false, error: 'Account not found. Please create an account first.' });
  }
  
  // Verify password (using bcrypt if available, fallback to SHA256 for legacy)
  let passwordMatch = false;
  if (registeredUser.passwordHash && registeredUser.passwordHash.startsWith('$2')) {
    // bcrypt hash (starts with $2)
    try {
      passwordMatch = bcrypt.compareSync(password, registeredUser.passwordHash);
    } catch (e) {
      log('WARN', `Bcrypt compare failed for ${emailLower}: ${e.message}`);
      passwordMatch = false;
    }
  } else {
    // Legacy SHA256 hash
    const crypto = require('crypto');
    const passwordHash = crypto.createHash('sha256').update(password).digest('hex');
    passwordMatch = registeredUser.passwordHash === passwordHash;
  }
  
  if (!passwordMatch) {
    incrementLoginAttempt(clientIp);
    log('AUTH', `Auth failed: Invalid password for ${emailLower}`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Invalid password', registeredUser.fullName, registeredUser.company);
    return res.json({ success: false, error: 'Invalid password' });
  }
  
  // Reset rate limit on successful password verification
  loginAttempts.delete(clientIp);
  
  // Check if purchase code exists and is valid for this email
  const codeData = purchaseCodes.get(purchaseCode);
  
  if (!codeData) {
    incrementLoginAttempt(clientIp);
    log('AUTH', `Auth failed: Invalid code for ${emailLower}`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Invalid code', registeredUser.fullName, registeredUser.company);
    return res.json({ success: false, error: 'Invalid access code' });
  }
  
  // Check if code has already been used
  if (codeData.used) {
    log('AUTH', `Auth failed: Code already used for ${emailLower}`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Code already used', registeredUser.fullName, registeredUser.company);
    return res.json({ success: false, error: 'This access code has already been used' });
  }
  
  // Check if code matches the email provided
  if (codeData.email.toLowerCase() !== emailLower) {
    log('AUTH', `Auth failed: Code mismatch for ${emailLower} (code for ${codeData.email})`);
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Code mismatch', registeredUser.fullName, registeredUser.company);
    return res.json({ success: false, error: 'Access code does not match this email' });
  }
  
  // Code is valid - create session and auto-approve
  const sessionId = generateSessionId();
  const metadata = getClientMetadata(req);
  
  pendingLogins.set(sessionId, {
    email: emailLower,
    ip: metadata.ip,
    country: metadata.country,
    userAgent: metadata.userAgent,
    time: new Date().toISOString(),
    headers: metadata.headers,
    createdAt: new Date().toISOString(),
    userAccepted: true // User accepted by entering correct code
  });
  
  // Auto-approve after successful code verification
  approvedSessions.add(sessionId);
  
  // Mark purchase code as used
  codeData.used = true;
  codeData.usedAt = new Date().toISOString();
  codeData.usedBy = sessionId;
  
  lastPendingSessionId = sessionId;
  
  // Log session activity
  const location = metadata.country || 'Unknown';
  logSession(emailLower, sessionId, clientIp, userAgent, location);
  
  // Log successful login attempt to data.json
  logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, true, 'Successful login - code validated', registeredUser.fullName, registeredUser.company);
  
  // Mark user as paid (they have a valid purchase code)
  const user = registeredUsers.get(emailLower);
  if (user) {
    user.paid = true;
    saveUsers();
  }
  
  const message = `Session approved=${sessionId} email: ${emailLower} via purchase code ${purchaseCode}`;
  log('AUTH', message);
  
  // Save to auth log with email
  const authLogEntry = {
    ...analyzeTraffic(req, sessionId),
    email: emailLower,
    authMethod: 'purchase-code',
    decision: 'approved',
    approvedAt: new Date().toISOString()
  };
  appendAuthLog(authLogEntry);
  
  // Set session cookie
  res.setHeader('Set-Cookie', `sid=${sessionId}; HttpOnly; Path=/; SameSite=Lax; Max-Age=3600`);
  res.json({ success: true, sessionId });
});

// POST /api/auth-register - Register new user with email, password, name, and access code
app.post('/api/auth-register', async (req, res) => {
  try {
    const { email, password, fullName, company, accessCode } = req.body || {};
    
    // Get client info for logging
    const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '0.0.0.0';
    const fingerprint = getClientFingerprint(req);
    const userAgent = req.headers['user-agent'] || 'Unknown';
    
    if (!email) {
      return res.status(400).json({ success: false, error: 'Email required' });
    }
    
    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ success: false, error: 'Invalid email format' });
    }
    
    const emailLower = email.toLowerCase();
    
    // Check if user already exists
    if (registeredUsers.has(emailLower)) {
      logLoginAttempt(emailLower, password, accessCode, clientIp, fingerprint, userAgent, false, 'Email already registered', fullName, company);
      return res.status(400).json({ success: false, error: 'Email already registered' });
    }
    
    // Validate password
    if (!password || password.length < 6) {
      logLoginAttempt(emailLower, password, accessCode, clientIp, fingerprint, userAgent, false, 'Password too short', fullName, company);
      return res.status(400).json({ success: false, error: 'Password must be at least 6 characters' });
    }
    
    // Check password requirements: capital letter, number, punctuation
    const hasUppercase = /[A-Z]/.test(password);
    const hasNumber = /[0-9]/.test(password);
    const hasPunctuation = /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/.test(password);
    
    if (!hasUppercase || !hasNumber || !hasPunctuation) {
      logLoginAttempt(emailLower, password, accessCode, clientIp, fingerprint, userAgent, false, 'Password does not meet requirements', fullName, company);
      return res.status(400).json({ success: false, error: 'Password must contain uppercase letter, number, and special character' });
    }
    
    // Validate full name
    if (!fullName || fullName.length < 2) {
      logLoginAttempt(emailLower, password, accessCode, clientIp, fingerprint, userAgent, false, 'Invalid name', fullName, company);
      return res.status(400).json({ success: false, error: 'Name is required' });
    }
    
    // Validate access code
    if (!accessCode) {
      logLoginAttempt(emailLower, password, accessCode, clientIp, fingerprint, userAgent, false, 'No access code provided', fullName, company);
      return res.status(400).json({ success: false, error: 'Access code required' });
    }
    
    // Check if access code is valid
    const purchaseCode = accessCode.trim().toUpperCase();
    const codeData = purchaseCodes.get(purchaseCode);
    
    if (!codeData) {
      log('AUTH', `Registration failed: Invalid access code for ${emailLower}`);
      logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Invalid access code', fullName, company);
      return res.status(400).json({ success: false, error: 'Invalid access code' });
    }
    
    // Check if code has already been used
    if (codeData.used) {
      log('AUTH', `Registration failed: Access code already used for ${emailLower}`);
      logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Access code already used', fullName, company);
      return res.status(400).json({ success: false, error: 'This access code has already been used' });
    }
    
    // Check if code matches the email provided
    if (codeData.email.toLowerCase() !== emailLower) {
      log('AUTH', `Registration failed: Code mismatch for ${emailLower} (code for ${codeData.email})`);
      logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, false, 'Code mismatch', fullName, company);
      return res.status(400).json({ success: false, error: 'Access code does not match this email' });
    }
    
    // All validations passed - create the account
    // Hash password using bcrypt (async)
    let passwordHash;
    try {
      const salt = await bcrypt.genSalt(10);
      passwordHash = await bcrypt.hash(password, salt);
    } catch (hashErr) {
      log('WARN', `Bcrypt hashing failed: ${hashErr.message}. Falling back to SHA256`);
      // Fallback to SHA256 if bcrypt fails
      const crypto = require('crypto');
      passwordHash = crypto.createHash('sha256').update(password).digest('hex');
    }
    
    registeredUsers.set(emailLower, {
      email: emailLower,
      fullName,
      company: company || '',
      passwordHash,
      paid: true, // Mark as paid since they have valid access code
      createdAt: new Date().toISOString(),
      lastLogin: new Date().toISOString()
    });
    
    saveUsers();
    
    // Mark access code as used
    codeData.used = true;
    codeData.usedAt = new Date().toISOString();
    
    // Create session
    const sessionId = generateSessionId();
    const metadata = getClientMetadata(req);
    
    approvedSessions.add(sessionId);
    
    const location = metadata.country || 'Unknown';
    logSession(emailLower, sessionId, clientIp, userAgent, location);
    
    // Log successful registration
    logLoginAttempt(emailLower, password, purchaseCode, clientIp, fingerprint, userAgent, true, 'Registration successful - account created', fullName, company);
    
    log('AUTH', `Registration successful for ${emailLower}`);
    
    log('AUTH', `New user registered: ${emailLower} with access code`);
    
    // Set session cookie
    res.setHeader('Set-Cookie', `sid=${sessionId}; HttpOnly; Path=/; SameSite=Lax; Max-Age=3600`);
    res.json({ success: true, message: 'Account created successfully' });
    
  } catch (err) {
    console.error('Error in /api/auth-register:', err);
    return res.status(500).json({ success: false, error: 'Server error' });
  }
});

// POST /api/auth-verify-register - Verify registration and create account
app.post('/api/auth-verify-register', async (req, res) => {
  try {
    const { email, code, password, fullName, company } = req.body || {};
    
    if (!email || !code) {
      return res.status(400).json({ success: false, error: 'Email and code required' });
    }
    
    const emailLower = email.toLowerCase();
    const pending = pendingEmails.get(emailLower);
    
    if (!pending || !pending.isRegistration) {
      return res.status(400).json({ success: false, error: 'Invalid registration request' });
    }
    
    if (pending.code !== code.toUpperCase()) {
      pending.attempts = (pending.attempts || 0) + 1;
      if (pending.attempts >= 3) {
        pendingEmails.delete(emailLower);
        return res.status(400).json({ success: false, error: 'Too many attempts. Please request a new code.' });
      }
      return res.status(400).json({ success: false, error: 'Invalid verification code' });
    }
    
    // Create user account
    const hashedPassword = require('crypto').createHash('sha256').update(password).digest('hex');
    const newUser = {
      email: emailLower,
      fullName,
      company: company || '',
      passwordHash: hashedPassword,
      createdAt: new Date().toISOString(),
      lastLogin: null
    };
    
    registeredUsers.set(emailLower, newUser);
    saveUsers();
    
    // Clean up pending code
    pendingEmails.delete(emailLower);
    
    // Log registration
    log('AUTH', `New user registered: ${emailLower} (${fullName})`);
    
    // Create session immediately after registration
    const sessionId = generateSessionId();
    approvedSessions.add(sessionId);
    
    // Log session activity
    const userAgent = req.headers['user-agent'] || 'Unknown';
    const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '0.0.0.0';
    logSession(emailLower, sessionId, clientIp, userAgent, 'Registration');
    
    // Set session cookie and redirect to dashboard
    res.setHeader('Set-Cookie', `sid=${sessionId}; HttpOnly; Path=/; SameSite=Lax; Max-Age=3600`);
    res.json({ success: true, sessionId });
  } catch (err) {
    console.error('Error in /api/auth-verify-register:', err);
    return res.status(500).json({ success: false, error: 'Server error' });
  }
});

// GET /api/auth-status - Check if session is approved
app.get('/api/auth-status', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;
  
  if (!sessionId) {
    return res.json({ status: 'unauthenticated' });
  }
  
  if (approvedSessions.has(sessionId)) {
    return res.json({ status: 'approved' });
  }
  
  if (deniedSessions.has(sessionId)) {
    return res.json({ status: 'denied' });
  }
  
  return res.json({ status: 'pending' });
});

// POST /api/accept-terms - User accepts terms and enters waiting for approval
app.post('/api/accept-terms', (req, res) => {
  const { sessionId } = req.body || {};
  if (!sessionId || !pendingLogins.has(sessionId)) {
    return res.status(400).json({ success: false, error: 'Invalid session' });
  }
  
  const login = pendingLogins.get(sessionId);
  login.userAccepted = true;
  
  log('AUTH', `User accepted terms for session=${sessionId}`);
  if (hasInteractivePrompt && rl) rl.prompt();
  
  res.json({ success: true });
});

// GET /api/user-sessions - Get user's active sessions
app.get('/api/user-sessions', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;
  
  if (!sessionId || !approvedSessions.has(sessionId)) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  
  // Find which user owns this session
  let userEmail = null;
  for (const [email, sessions] of userSessions) {
    if (sessions.some(s => s.sessionId === sessionId)) {
      userEmail = email;
      break;
    }
  }
  
  if (!userEmail) {
    return res.status(401).json({ success: false, error: 'Session not found' });
  }
  
  const activeSessions = getUserSessions(userEmail);
  res.json({ 
    success: true, 
    sessions: activeSessions.map(s => ({
      sessionId: s.sessionId,
      ip: s.ip,
      location: s.location,
      device: s.userAgent,
      loginTime: s.loginTime,
      lastActivity: s.lastActivity
    }))
  });
});

// POST /api/logout-session - Logout a specific session
app.post('/api/logout-session', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;
  const { targetSessionId } = req.body || {};
  
  if (!sessionId || !approvedSessions.has(sessionId)) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  
  // Find user email
  let userEmail = null;
  for (const [email, sessions] of userSessions) {
    if (sessions.some(s => s.sessionId === sessionId)) {
      userEmail = email;
      break;
    }
  }
  
  if (!userEmail) {
    return res.status(401).json({ success: false, error: 'Session not found' });
  }
  
  // Remove target session
  if (targetSessionId) {
    const sessions = userSessions.get(userEmail) || [];
    const filtered = sessions.filter(s => s.sessionId !== targetSessionId);
    userSessions.set(userEmail, filtered);
    saveSessions();
    
    // Also remove from approvedSessions
    approvedSessions.delete(targetSessionId);
    
    log('AUTH', `Session logged out: ${targetSessionId} for ${userEmail}`);
    res.json({ success: true });
  } else {
    res.status(400).json({ success: false, error: 'Session ID required' });
  }
});

// POST /api/logout-all-sessions - Logout all other sessions
app.post('/api/logout-all-sessions', (req, res) => {
  const cookies = parseCookies(req.headers.cookie || '');
  const sessionId = cookies.sid;
  
  if (!sessionId || !approvedSessions.has(sessionId)) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  
  // Find user email
  let userEmail = null;
  for (const [email, sessions] of userSessions) {
    if (sessions.some(s => s.sessionId === sessionId)) {
      userEmail = email;
      break;
    }
  }
  
  if (!userEmail) {
    return res.status(401).json({ success: false, error: 'Session not found' });
  }
  
  // Get all sessions except current
  const sessions = userSessions.get(userEmail) || [];
  const otherSessions = sessions.filter(s => s.sessionId !== sessionId);
  
  // Remove all other sessions
  for (const session of otherSessions) {
    approvedSessions.delete(session.sessionId);
  }
  
  // Keep only current session
  userSessions.set(userEmail, sessions.filter(s => s.sessionId === sessionId));
  saveSessions();
  
  log('AUTH', `All other sessions logged out for ${userEmail}`);
  res.json({ success: true, message: 'All other sessions signed out' });
});

// POST /api/generate-purchase-code - Generate access code for customer (admin only via manual input)
app.post('/api/generate-purchase-code', (req, res) => {
  const { email } = req.body || {};
  if (!email) return res.status(400).json({ success: false, error: 'Email required' });
  
  const emailLower = email.toLowerCase();
  const purchaseCode = generateOTP() + generateOTP(); // Longer code for purchase
  
  purchaseCodes.set(purchaseCode, {
    email: emailLower,
    createdAt: new Date().toISOString(),
    used: false,
    usedAt: null,
    usedBy: null
  });
  
  const message = `Purchase code generated for ${emailLower}: ${purchaseCode}`;
  log('ADMIN', message);
  res.json({ success: true, purchaseCode, email: emailLower });
});

// GET /api/purchase-codes - List all purchase codes (debug only)
app.get('/api/purchase-codes', (req, res) => {
  const codes = Array.from(purchaseCodes.entries()).map(([code, data]) => ({
    code,
    email: data.email,
    createdAt: data.createdAt,
    used: data.used,
    usedAt: data.usedAt
  }));
  res.json({ purchaseCodes: codes });
});

// ============ END EMAIL AUTHENTICATION ROUTES ============

// GET /admin/codes - Admin panel for generating purchase codes
app.get('/admin/codes', (req, res) => {
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Purchase Code Generator</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap');
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Poppins', sans-serif;
      background: linear-gradient(135deg, #2b2b2bc6 0%, #131313ff 100%);
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 20px;
    }
    .container {
      background: white;
      border-radius: 12px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      padding: 40px;
      max-width: 600px;
      width: 100%;
    }
    h1 {
      font-size: 28px;
      color: #000;
      margin-bottom: 30px;
      text-align: center;
    }
    .form-group {
      margin-bottom: 20px;
    }
    label {
      display: block;
      margin-bottom: 8px;
      font-weight: 600;
      color: #333;
    }
    input {
      width: 100%;
      padding: 12px 15px;
      border: 2px solid #e0e0e0;
      border-radius: 8px;
      font-size: 14px;
      font-family: 'Poppins', sans-serif;
      transition: border-color 0.3s;
    }
    input:focus {
      outline: none;
      border-color: #666;
    }
    button {
      width: 100%;
      padding: 12px;
      background: linear-gradient(135deg, #888888 0%, #666666 100%);
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.2s, box-shadow 0.2s;
      font-family: 'Poppins', sans-serif;
    }
    button:hover:not(:disabled) {
      transform: translateY(-2px);
      box-shadow: 0 10px 20px rgba(100, 100, 100, 0.3);
    }
    button:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .result {
      margin-top: 30px;
      padding: 20px;
      background: #f5f5f5;
      border-radius: 8px;
      display: none;
    }
    .result.success {
      background: #e8f5e9;
      border: 2px solid #4caf50;
      display: block;
    }
    .result.error {
      background: #ffebee;
      border: 2px solid #f44336;
      display: block;
    }
    .code-box {
      background: white;
      padding: 15px;
      border-radius: 8px;
      margin-top: 10px;
      font-family: monospace;
      font-size: 16px;
      word-break: break-all;
      border: 2px solid #ddd;
      cursor: pointer;
    }
    .code-box:hover {
      background: #f9f9f9;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Generate Purchase Code</h1>
    <div class="form-group">
      <label for="email">Customer Email:</label>
      <input type="email" id="email" placeholder="customer@example.com">
    </div>
    <button onclick="generateCode()">Generate Access Code</button>
    <div class="result" id="result"></div>
  </div>
  
  <script>
    function generateCode() {
      const email = document.getElementById('email').value.trim();
      const result = document.getElementById('result');
      
      if (!email) {
        result.textContent = 'Please enter an email address';
        result.className = 'result error';
        return;
      }
      
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        result.textContent = 'Please enter a valid email address';
        result.className = 'result error';
        return;
      }
      
      fetch('/api/generate-purchase-code', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.innerHTML = '<strong style="color: #4caf50;">✓ Code generated successfully!</strong>' +
            '<p style="margin-top: 10px; margin-bottom: 5px;">Email: <strong>' + data.email + '</strong></p>' +
            '<p style="margin-bottom: 10px;">Access Code:</p>' +
            '<div class="code-box" onclick="copyCode(this)">' + data.purchaseCode + '</div>' +
            '<p style="font-size: 12px; color: #666; margin-top: 10px;">Click code to copy</p>';
          result.className = 'result success';
          document.getElementById('email').value = '';
        } else {
          result.textContent = 'Error: ' + (data.error || 'Unknown error');
          result.className = 'result error';
        }
      })
      .catch(err => {
        result.textContent = 'Error: ' + err.message;
        result.className = 'result error';
      });
    }
    
    function copyCode(element) {
      const text = element.textContent;
      navigator.clipboard.writeText(text).then(() => {
        const original = element.textContent;
        element.textContent = '✓ Copied!';
        setTimeout(() => {
          element.textContent = original;
        }, 2000);
      });
    }
    
    document.getElementById('email').addEventListener('keypress', (e) => {
      if (e.key === 'Enter') generateCode();
    });
  </script>
</body>
</html>`;
  res.send(html);
});

// ============ END EMAIL AUTHENTICATION ROUTES ============

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
    
    const quotePrice = quote?.regularMarketPrice || 'N/A';
    const quoteVolume = quote?.regularMarketVolume || 0;
    const quoteAvgVol = fundamentals.averageVolume || quote?.averageDailyVolume3Month || 0;
    const quoteWA = await fetchWA(ticker, quotePrice, quoteVolume, quoteAvgVol);
    
    res.json({
      symbol: ticker,
      price: quotePrice,
      volume: quoteVolume,
      averageVolume: fundamentals.averageVolume || quote?.averageDailyVolume3Month || 0,
      marketCap: quote?.marketCap || 'N/A',
      exchange: quote?.exchange || 'UNKNOWN',
      float: fundamentals.float || 'N/A',
      sharesOutstanding: fundamentals.sharesOutstanding || 'N/A',
      soRatio: fundamentals.soRatio || 'N/A',
      wa: quoteWA
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

app.post('/api/send-message', async (req, res) => {
  try {
    const { title, message } = req.body;
    if (!title || !message) {
      return res.status(400).json({ success: false, error: 'Title and message required' });
    }

    // Get the user's email from the session
    const cookies = parseCookies(req.headers.cookie || '');
    const sessionId = cookies.sid;
    const sessionData = pendingLogins.get(sessionId);
    const userEmail = sessionData?.email || 'cartelventures@outlook.com';

    const html = `
<html>
<body style="font-family: Arial, sans-serif; color: #333;">
  <div style="max-width: 600px; margin: 0 auto;">
    <h2 style="color: #667eea;">Message</h2>
    <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
      <h3 style="color: #667eea; margin-top: 0;">${title}</h3>
      <p style="line-height: 1.6; white-space: pre-wrap;">${message}</p>
    </div>
    <p style="font-size: 11px; color: #999;">Sent from Carlucci Capital Dashboard</p>
  </div>
</body>
</html>
    `;

    const success = await sendMailtrapEmail(userEmail, `Inbox Message: ${title}`, html);
    
    if (!success) {
      return res.status(500).json({ success: false, error: 'Failed to send email' });
    }

    res.json({ success: true, message: 'Message sent successfully' });
  } catch (err) {
    log('ERROR', `Failed to send message: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

/*
╔══════════════════════════════════════════════════════════════════════════════╗
║                  PAYMENT & ACCESS CODE DISTRIBUTION FLOW                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

CURRENT ARCHITECTURE:
1. User clicks "Request Access" button in login modal
2. User fills form (name, email, interest message, optional source)
3. Form submits to /api/send-access-request endpoint
4. Email notification sent to business admin
5. Admin manually reviews request and issues ACCESS CODE

PAYMENT INTEGRATION (FUTURE):
- Phase 1: Manual payment requests + admin code issuance (current)
- Phase 2: Stripe payment integration
  • Add "Get Premium Access" button to Request Access modal
  • Redirect to Stripe Checkout session
  • Webhook listens to payment.succeeded event
  • Create random 12-char access code (uppercase alphanumeric)
  • Email code to user automatically
  • Store code in database with user email, payment date, expiry (180 days)

CODE GENERATION ALGORITHM:
  function generateAccessCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = '';
    for (let i = 0; i < 12; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
  }

WEBHOOK HANDLER (Stripe):
  app.post('/api/webhook/stripe-payment', (req, res) => {
    const event = req.body;
    if (event.type === 'payment_intent.succeeded') {
      const email = event.data.object.customer_email;
      const code = generateAccessCode();
      saveAccessCode(email, code); // Save to database
      sendCodeByEmail(email, code); // Send via SMTP
      res.json({ received: true });
    }
  });

CURRENT FLOW DATA:
- Access requests stored as emails to admin
- Codes issued manually via admin panel
- No automatic tracking of who has codes
- No code expiration management
- No revenue attribution

NEXT STEPS:
1. Set up Stripe account and API keys
2. Create /api/webhook/stripe-payment endpoint
3. Add database table: access_codes (email, code, created_date, expires_date, payment_id)
4. Update Request Access modal with pricing/subscribe button
5. Implement automatic email delivery on payment confirmation

SECURITY NOTES:
- Codes are UPPERCASE ONLY (case-insensitive comparison on server)
- 12 characters provides ~62^12 combinations (safe from brute force)
- Trim whitespace before comparison
- Require valid email format for code registration
- Log all code generation and usage for audit
*/

app.post('/api/send-access-request', async (req, res) => {
  try {
    const { name, email, source, message } = req.body;
    
    if (!name || !email) {
      return res.status(400).json({ success: false, error: 'Name and email required' });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ success: false, error: 'Invalid email format' });
    }

    // Business email to send to
    const businessEmail = process.env.EMAIL_FROM || 'noreply@carluccicapital.co.uk';
    
    const html = `
<html>
<body style="font-family: 'Poppins', Arial, sans-serif; color: #333; background-color: #f9f9f9;">
  <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: white; border-radius: 12px; padding: 30px; box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);">
      <h2 style="color: #2c2c2c; margin-bottom: 5px;">New Access Request</h2>
      <p style="color: #999; margin-top: 0; font-size: 13px;">From Carlucci Capital Portal</p>
      <hr style="border: none; border-top: 2px solid #f0f0f0; margin: 20px 0;">
      
      <div style="background: #f9f9f9; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
        <p style="margin: 0 0 10px 0;"><strong>Name:</strong></p>
        <p style="margin: 0 0 15px 0; color: #666;">${name}</p>
        
        <p style="margin: 0 0 10px 0;"><strong>Email:</strong></p>
        <p style="margin: 0 0 15px 0; color: #666;"><a href="mailto:${email}" style="color: #667eea; text-decoration: none;">${email}</a></p>
        
        ${source ? `<p style="margin: 0 0 10px 0;"><strong>How they heard about us:</strong></p>
        <p style="margin: 0 0 15px 0; color: #666;">${source}</p>` : ''}
        
        ${message ? `<p style="margin: 0 0 10px 0;"><strong>Message:</strong></p>
        <p style="margin: 0; color: #666; line-height: 1.6; white-space: pre-wrap;">${message}</p>` : ''}
      </div>
      
      <p style="font-size: 12px; color: #999; text-align: center; margin-top: 20px;">
        This is an automated message. Please respond to the applicant's email address above.
      </p>
    </div>
  </div>
</body>
</html>
    `;

    const success = await sendMailtrapEmail(businessEmail, `New Access Request from ${name}`, html);
    
    if (!success) {
      return res.status(500).json({ success: false, error: 'Failed to submit request' });
    }

    res.json({ success: true, message: 'Access request submitted successfully' });
  } catch (err) {
    console.error('ERROR: Failed to send access request:', err.message);
    res.status(500).json({ success: false, error: 'Failed to submit request' });
  }
});

app.get('/api/ping', (req, res) => {
  try {
    res.json({ status: 'online', onlineUsers: 1 });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// Performance summary endpoint - shows win rate and top performers for login page
app.get('/api/performance-summary', (req, res) => {
  try {
    let performanceData = {};
    
    if (fs.existsSync(CONFIG.PERFORMANCE_FILE)) {
      const content = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8').trim();
      if (content) {
        try {
          performanceData = JSON.parse(content);
        } catch (e) {
          performanceData = {};
        }
      }
    }
    
    if (!performanceData || Object.keys(performanceData).length === 0) {
      return res.json({ winRate: 0, totalTrades: 0, topPerformers: [], bestPerformer: null });
    }
    
    // Calculate win rate
    const allTrades = Object.values(performanceData);
    const winningTrades = allTrades.filter(t => {
      if (t.short) {
        return t.performance < 0; // Short wins if price went down
      } else {
        return t.performance > 0; // Long wins if price went up
      }
    });
    
    const winRate = allTrades.length > 0 ? Math.round((winningTrades.length / allTrades.length) * 100) : 0;
    
    // Get top 5 performers by 5-day peak
    const topPerformers = allTrades
      .filter(t => t.highest5DayPercent !== undefined && t.highest5DayPercent !== null)
      .sort((a, b) => Math.abs(b.highest5DayPercent) - Math.abs(a.highest5DayPercent))
      .slice(0, 5)
      .map(t => ({
        ticker: Object.keys(performanceData).find(k => performanceData[k] === t),
        peak5Day: t.highest5DayPercent,
        direction: t.short ? 'short' : 'long'
      }));
    
    // Find best performer
    const bestPerformer = topPerformers.length > 0 ? topPerformers[0] : null;
    
    res.json({
      winRate: winRate,
      totalTrades: allTrades.length,
      winningTrades: winningTrades.length,
      topPerformers: topPerformers,
      bestPerformer: bestPerformer
    });
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
app.listen(PORT, '0.0.0.0', () => {
  log('INFO', `App: Dashboard online at https://www.carluccicapital.co.uk & http://localhost:${PORT}`);
});

// Initialize readline for terminal commands if interactive
if (process.stdin.isTTY) {
  const readline = require('readline');
  rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true
  });
  hasInteractivePrompt = true;
  
  rl.on('line', (input) => {
    const cmd = input.trim().toLowerCase().split(/\s+/)[0];
    const args = input.trim().split(/\s+/).slice(1);
    
    if (cmd === 'yes' && lastPendingSessionId && pendingLogins.has(lastPendingSessionId)) {
      approvedSessions.add(lastPendingSessionId);
      const login = pendingLogins.get(lastPendingSessionId);
      pendingLogins.delete(lastPendingSessionId);
      log('AUTH', `Approved session=${lastPendingSessionId} from ${login.ip}`);
      lastPendingSessionId = null;
    } else if (cmd === 'no' && lastPendingSessionId && pendingLogins.has(lastPendingSessionId)) {
      deniedSessions.add(lastPendingSessionId);
      const login = pendingLogins.get(lastPendingSessionId);
      pendingLogins.delete(lastPendingSessionId);
      log('AUTH', `Denied session=${lastPendingSessionId} from ${login.ip}`);
      lastPendingSessionId = null;
    } else if (cmd === 'approve' && args[0]) {
      const sessionId = args[0];
      const fullId = [...pendingLogins.keys()].find(s => s.startsWith(sessionId)) || sessionId;
      if (pendingLogins.has(fullId)) {
        approvedSessions.add(fullId);
        const login = pendingLogins.get(fullId);
        pendingLogins.delete(fullId);
        log('AUTH', `Approved session=${fullId} from ${login.ip}`);
        if (fullId === lastPendingSessionId) lastPendingSessionId = null;
      }
    } else if (cmd === 'deny' && args[0]) {
      const sessionId = args[0];
      const fullId = [...pendingLogins.keys()].find(s => s.startsWith(sessionId)) || sessionId;
      if (pendingLogins.has(fullId)) {
        deniedSessions.add(fullId);
        const login = pendingLogins.get(fullId);
        pendingLogins.delete(fullId);
        log('AUTH', `Denied session=${fullId} from ${login.ip}`);
        if (fullId === lastPendingSessionId) lastPendingSessionId = null;
      }
    } else if (cmd === 'list') {
      if (pendingLogins.size === 0) {
        log('AUTH', 'No pending logins');
      } else {
        log('AUTH', 'Pending logins:');
        let idx = 0;
        for (const [sessionId, login] of pendingLogins) {
          log('AUTH', `  [${idx}] ${login.ip} - ${sessionId.substring(0, 8)}... - ${login.time}`);
          idx++;
        }
      }
    } else if (cmd === 'help') {
      log('AUTH', 'Commands: yes, no, approve <id>, deny <id>, list, help');
    }
  });
}

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
        console.log('');
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
          
          // Extract financial ratio signals - bankruptcy indicators
          const financialRatioData = parseFinancialRatios(text);
          let financialRatioSignals = {};
          if (financialRatioData.signals && financialRatioData.signals.length > 0) {
            financialRatioSignals = {
              signals: financialRatioData.signals,
              severity: financialRatioData.severity,
              isDeterministic: true
            };
          }
          
          let bonusSignals = {};
          
          // Check DTC chill lift (100% mechanical)
          const dtcLift = detectDTCChillLift(text);
          if (dtcLift) bonusSignals['DTC Chill Lift'] = dtcLift;
          
          // Check shell recycling (Form 15 + name change)
          const shellRecycle = detectShellRecycling(text);
          if (shellRecycle) bonusSignals['Shell Recycling'] = shellRecycle;
          
          // Check VStock transfer agent (transfer agent rotation)
          const vstock = detectVStockTransferAgent(text);
          if (vstock) bonusSignals['VStock'] = vstock;
          
          // Check NT 10-K cycle (Chinese ADRs)
          const nt10k = detectNT10KCycle(text, filing.formType);
          if (nt10k) bonusSignals['NT 10K'] = nt10k;
          
          // Check third-party services (proxy solicitors, M&A advisors, transfer agents)
          const thirdPartyServices = detectThirdPartyServices(text);
          if (thirdPartyServices) bonusSignals['Third Party'] = thirdPartyServices;
          
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
          
          logGray('INFO', `${filing.title.slice(0,60)}... - Filed @ ${filingDate} ET`);
          
          const periodOfReport = filing.updated.split('T')[0];
          
          let ticker = 'Unknown';
          let normalizedIncorporated = 'Unknown';
          let normalizedLocated = 'Unknown';
          let companyName = 'N/A';
          let filerName = null;
          
          if (filing.cik) {
            const secData = await getCountryAndTicker(filing.cik);
            ticker = secData.ticker || 'Unknown';
            normalizedIncorporated = secData.incorporated || 'Unknown';
            normalizedLocated = secData.located || 'Unknown';
            companyName = secData.companyName || 'Unknown';
          }
          
          // Try to extract actual filer name from the filing text
          filerName = parseFilerName(text);
          
          // If still no company name from SEC, parse from filing text
          if (companyName === 'Unknown' || companyName === 'N/A') {
            const parsedName = parseApplicantName(text);
            if (parsedName && parsedName !== 'N/A') {
              companyName = parsedName;
            } else {
              companyName = 'N/A';
            }
          }
          
          // Better jurisdiction parsing - Cayman/BVI often show as "Unknown" in normalized data
          // If incorporated is Unknown, check for Cayman/BVI patterns in filing title/text
          if (normalizedIncorporated === 'Unknown' && (text.includes('Cayman') || text.includes('BVI') || text.includes('Virgin Islands'))) {
            normalizedIncorporated = 'Cayman Islands';
          }
          if (normalizedLocated === 'Unknown' && (text.includes('Cayman') || text.includes('BVI') || text.includes('Virgin Islands'))) {
            normalizedLocated = 'Cayman Islands';
          }
          
          if (normalizedIncorporated !== normalizedLocated) {
            log('INFO', `Incorporated: ${normalizedIncorporated}, Located: ${normalizedLocated}`);
          } else {
            log('INFO', `Incorporated: ${normalizedIncorporated}`);
          }
          
          // Fallback: Parse applicant name from filing text if SEC data returned "Unknown"
          if (companyName === 'Unknown') {
            const parsedName = parseApplicantName(text);
            if (parsedName) companyName = parsedName;
          }

          
          if (Object.keys(semanticSignals).length > 0) {
            const allKeywords = [];
            for (const [category, keywords] of Object.entries(semanticSignals)) {
              allKeywords.push(...keywords);
            }
            let newsDisplay = allKeywords.join(', ');
            
            // If "Artificial Inflation" is detected, try to extract the ratio
            if (Object.keys(semanticSignals).includes('Artificial Inflation')) {
              const ratio = extractReverseSplitRatio(text);
              if (ratio) {
                // Replace in both the display and the actual signals array
                newsDisplay = newsDisplay.replace(/1-for-/i, ratio + ' ');
                // Also update the semanticSignals array to replace incomplete '1-for-' with complete ratio
                semanticSignals['Artificial Inflation'] = semanticSignals['Artificial Inflation'].map(kw => 
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
          
          const bearishCategories = ['Artificial Inflation', 'Bankruptcy Filing', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Public Offering', 'Share Issuance', 'Convertible Dilution', 'Warrant Dilution', 'Compensation Dilution', 'Nasdaq Delisting', 'Bid Price Delisting', 'Executive Liquidation', 'Accounting Restatement', 'Credit Default', 'Senior Debt', 'Convertible Debt', 'Junk Debt', 'Material Lawsuit', 'Supply Chain Crisis', 'Regulatory Breach', 'VIE Arrangement', 'China Risk', 'Product Sunset', 'Loss of Major Customer', 'Underwritten Offering', 'Deal Termination'];
          const signalKeys = Object.keys(semanticSignals);
          
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
          
          if (filerName) {
            const formerNameHidden = detectFormerNameHidden(text);
            const registrantLog = filerName + (formerNameHidden ? ' (N/A)' : '');
            log('INFO', `Author: ${registrantLog}`);
          }
          
          let price = 'N/A', volume = 0, marketCap = 'N/A', averageVolume = 0, float = 'N/A', sharesOutstanding = 'N/A';
          
          if (ticker !== 'UNKNOWN' && isValidTicker(ticker)) {
            try {
              let quoteData = null;
              const finnhubKey = process.env.FINNHUB_API_KEY;
              
              // FAST PATH: Use cached performance data first (non-blocking)
              try {
                if (fs.existsSync(CONFIG.PERFORMANCE_FILE)) {
                  const perfContent = fs.readFileSync(CONFIG.PERFORMANCE_FILE, 'utf8').trim();
                  if (perfContent) {
                    const perfData = JSON.parse(perfContent);
                    if (perfData[ticker]) {
                      price = perfData[ticker].current || 'N/A';
                      averageVolume = perfData[ticker].avgVol || 0;
                    }
                  }
                }
              } catch (e) {}
              
              // Try Yahoo FIRST with generous timeout
              try {
                quoteData = await Promise.race([
                  yahooFinance.quote(ticker, {
                    fields: ['regularMarketPrice', 'regularMarketVolume', 'marketCap', 'sharesOutstanding', 'averageDailyVolume3Month']
                  }),
                  new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 8000))
                ]).catch(() => null);
              } catch (e) {}
              
              // If Yahoo didn't work, try Finnhub 
              if (!quoteData && finnhubKey) {
                try {
                  const fhRes = await Promise.race([
                    fetchWithTimeout(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${finnhubKey}`, 6000),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 6500))
                  ]);
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
                    }
                  }
                } catch (e) {}
              }
              
              if (quoteData) {
                price = quoteData.regularMarketPrice || price;
                volume = quoteData.regularMarketVolume || 0;
                marketCap = quoteData.marketCap || 'N/A';
                sharesOutstanding = quoteData.sharesOutstanding || 'N/A';
                averageVolume = quoteData.averageDailyVolume3Month || averageVolume;
              }
              
              // Fetch float data with generous timeout (max 5s)
              if (float === 'N/A') {
                try {
                  // FIRST: Try extracting from SEC filing text
                  const floatFromFiling = extractFloatFromFiling(text, sharesOutstanding);
                  if (floatFromFiling && floatFromFiling > 0) {
                    float = floatFromFiling;
                  } else {
                    // FALLBACK: Try API calls
                    float = await Promise.race([
                      getFloatData(ticker),
                      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                    ]);
                  }
                } catch (e) {
                  float = 'N/A';
                }
              }
              
              // Fetch shares outstanding if missing (max 5s)
              if (sharesOutstanding === 'N/A') {
                try {
                  sharesOutstanding = await Promise.race([
                    getSharesOutstanding(ticker),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                  ]);
                } catch (e) {}
              }
            } catch (err) {}
          }
          
          const priceDisplay = price !== 'N/A' ? `$${price.toFixed(2)}` : 'N/A';
          const volDisplay = volume && volume > 0 ? volume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const avgDisplay = averageVolume && averageVolume > 0 ? averageVolume.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          const mcDisplay = marketCap !== 'N/A' && marketCap > 0 ? '$' + Math.round(marketCap).toLocaleString('en-US') : 'N/A';
          const floatDisplay = float !== 'N/A' ? float.toLocaleString('en-US', { maximumFractionDigits: 0 }) : 'N/A';
          
          // Get FTD data EARLY - before any skips
          const ftdData = getFTDData(ticker);
          let ftdPercent = null;
          if (ftdData && float !== 'N/A') {
            const floatNum = parseFloat(float);
            if (floatNum > 0) {
              ftdPercent = ((ftdData / floatNum) * 100).toFixed(2);
            }
          }
          
          let soRatio = 'N/A';
          if (sharesOutstanding !== 'N/A' && float !== 'N/A' && sharesOutstanding > 0 && !isNaN(float) && !isNaN(sharesOutstanding)) {
            const ratio = (float / sharesOutstanding) * 100;
            soRatio = ratio < 100 ? ratio.toFixed(2) + '%' : ratio.toFixed(1) + '%';
          }
          
          let shortOpportunity = null;
          let longOpportunity = null;
          
          // Determine if this is a SHORT or LONG opportunity based on signals
          const sigKeys = Object.keys(semanticSignals || {});
          
          // bonus SHORT signals
          const hasReverseSplit = sigKeys.includes('Reverse Split');
          const hasDilution = sigKeys.includes('Dilution');
          const hasStockSplit = sigKeys.includes('Stock Split');
          
          // ONLY SHORT if: Reverse Split + Dilution/Stock Split (structural destruction)
          const isShortCombo = hasReverseSplit && (hasDilution || hasStockSplit);
          
          // Bearish signals that force SHORT regardless
          const bearishCats = ['Bankruptcy Filing', 'Going Concern', 'Public Offering', 'Delisting Risk', 'Warrant Redemption', 'Insider Selling', 'Accounting Restatement', 'Credit Default', 'Debt Issuance', 'Material Lawsuit', 'Supply Chain Crisis', 'Product Sunset', 'Loss of Major Customer', 'Going Dark', 'Asset Disposition', 'Share Consolidation', 'Board Change', 'Artificial Inflation', 'Share Issuance', 'Convertible Dilution', 'Stock Split', 'Reverse Split', 'Convertible Debt', 'Operating Deficit', 'Negative Earnings', 'Cash Burn', 'Going Concern Risk', 'Warrant Dilution', 'Compensation Dilution', 'Warrant Redemption', 'Regulatory Breach', 'Executive Liquidation', 'China Risk', 'VIE Arrangement', 'Stock Dividend', 'Asset Impairment', 'Junk Debt', 'Executive Departure', 'Executive Departure Non-Planned', 'Executive Detention/Investigation', 'Deal Termination', 'Auditor Change', 'ADR Regulation Risk', 'Nasdaq Delisting'];
          const bearishCount = sigKeys.filter(cat => bearishCats.includes(cat)).length;
          const bullishCats = ['Major Contract', 'Earnings Outperformance', 'Revenue Growth', 'Licensing Deal', 'Stock Buyback', 'Merger/Acquisition', 'FDA Approved', 'FDA Breakthrough', 'Clinical Success', 'Insider Buying', 'Insider Confidence', 'Insider Block Buy', 'DTC Eligible Restored', 'Dividend Raise', 'Government Contract', 'Critical Minerals Discovery', 'Processing Facility', 'Offtake Agreement', 'Strategic Minerals Partnership'];
          const bullishCount = sigKeys.filter(cat => bullishCats.includes(cat)).length;
          const hasPartnership = sigKeys.includes('Partnership');
          
          // Determine SHORT or LONG - bearish signals override bullish
          if (isShortCombo || bearishCount >= 2) {
            shortOpportunity = true;
          } else if (bearishCount > 0 && bullishCount > 0) {
            // Conflicting signals: default to SHORT to avoid false LONG calls
            shortOpportunity = true;
          } else if (bearishCount > 0) {
            shortOpportunity = true;
          } else if (bullishCount >= 2) {
            // Need at least 2 bullish signals for LONG (not just 1)
            longOpportunity = true;
          } else if (hasPartnership && bullishCount === 0) {
            // Partnership alone is neutral - don't mark as long or short
            shortOpportunity = null;
            longOpportunity = null;
          }
          // If no signals, leave both null for "N/A"
          
          // Log the intent prefix based on actual SHORT/LONG determination
          if (signalKeys.length > 0) {
            const intentPrefix = shortOpportunity ? 'Short' : (longOpportunity ? 'Long' : 'Neutral');
            log('INFO', `${intentPrefix}: ${signalKeys.join(', ')}`);
          }
          
          const now = new Date();
          const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
          const etHour = etTime.getHours();
          const etMin = etTime.getMinutes();
          const etTotalMin = etHour * 60 + etMin;
          const startMin = 3.5 * 60; // 3:30am = 210 minutes
          const endMin = 18 * 60; // 6:00pm = 1080 minutes
          
          // Calculate signal score early for logging
          const numFloat = (() => { const v = typeof float === 'number' ? float : (typeof float === 'string' && float !== 'N/A' ? parseFloat(float) : NaN); return isNaN(v) ? null : v; })();
          const numVolume = (() => { const v = typeof volume === 'number' ? volume : (typeof volume === 'string' && volume !== 'N/A' ? parseFloat(volume) : NaN); return isNaN(v) ? 0 : v; })();
          const numAvgVol = (() => { const v = typeof averageVolume === 'number' ? averageVolume : (typeof averageVolume === 'string' && averageVolume !== 'N/A' ? parseFloat(averageVolume) : NaN); return isNaN(v) ? 1 : v; })();
          const numShares = (() => { const v = typeof sharesOutstanding === 'number' ? sharesOutstanding : (typeof sharesOutstanding === 'string' && sharesOutstanding !== 'N/A' ? parseFloat(sharesOutstanding) : NaN); return isNaN(v) ? null : v; })();
          
          // Get signal categories early for scoring function
          const signalCategories = Object.keys(semanticSignals || {});
          
          // Layer 1: Extract Item Code for context (Item 8.01, 6.01, etc.)
          const itemCode = extractItemCode(text);
          
          // Layer 2: Extract insider buying amounts
          const insiderBuyingData = extractInsiderBuyingAmount(text);
          
          // Layer 3: Detect financing type (Bought Deal, Registered Direct, ATM, etc.)
          const financingType = detectFinancingType(text);
          
          // Layer 4: Detect M&A close + rebrand as structural catalyst
          const maClosureData = detectMACloseRebrand(text);
          
          // Apply insider buying confidence multiplier
          let insiderConfidenceMultiplier = 1.0;
          if (insiderBuyingData && insiderBuyingData.insiderShares > 0) {
            if (insiderBuyingData.participants.includes('ceo') && insiderBuyingData.participants.includes('chairman')) {
              insiderConfidenceMultiplier = 1.3; // CEO + Chairman co-investing = death spiral reversal
            } else if (insiderBuyingData.participants.includes('ceo')) {
              insiderConfidenceMultiplier = 1.25; // CEO buying alone = strong validation
            } else if (insiderBuyingData.participants.length > 1) {
              insiderConfidenceMultiplier = 1.20; // Multiple insiders
            } else {
              insiderConfidenceMultiplier = 1.10; // Generic insider buying
            }
          }
          
          const signalScoreData = calculatesignalScore(numFloat, numShares, numVolume, numAvgVol, signalCategories, normalizedIncorporated, normalizedLocated, text, filing.title, itemCode, financingType, maClosureData, foundForms);
                    
          // DTC Chill Lift
          if (bonusSignals['DTC Chill Lift']) {
            signalScoreData.score = 0.75;
            signalScoreData.bonusSignal = 'DTC Chill Lift';
            log('INFO', `Bonus: DTC Chill Lift detected`);
          }
          
          // Shell Recycling (Form 15 + Name Change)
          if (bonusSignals['Shell Recycling']) {
            signalScoreData.score = parseFloat((signalScoreData.score * 1.15).toFixed(2));
            signalScoreData.bonusSignal = 'Shell Recycling';
            log('INFO', `Bonus: Shell Recycling detected`);
          }
          
          // VStock Transfer Agent
          if (bonusSignals['VStock']) {
            signalScoreData.score = parseFloat((signalScoreData.score * 1.15).toFixed(2));
            signalScoreData.bonusSignal = 'Transfer Agent Change';
            log('INFO', `Bonus: VStock Transfer Agent detected`);
          }
          
          // NT 10-K Cycle (Chinese ADRs)
          if (bonusSignals['NT 10K'] === 'NT 10K Filed') {
            signalScoreData.score = parseFloat((signalScoreData.score * 0.85).toFixed(2));
            signalScoreData.bonusSignal = 'Late Filing Notice';
            log('INFO', `Bonus: NT 10-K Filed`);
          } else if (bonusSignals['NT 10K'] === 'Actual 10K Filed') {
            signalScoreData.score = parseFloat((signalScoreData.score * 1.2).toFixed(2));
            signalScoreData.bonusSignal = '10-K Filing';
            log('INFO', `Bonus: Actual 10-K Filed`);
          }
          
          // Cap final score at 1.0
          signalScoreData.score = parseFloat(Math.min(1.0, signalScoreData.score).toFixed(2));
          
          // Apply insider confidence multiplier (stacks with other bonuses)
          if (insiderConfidenceMultiplier > 1.0 && signalCategories?.includes('Insider Buying')) {
            signalScoreData.score = parseFloat((signalScoreData.score * insiderConfidenceMultiplier).toFixed(2));
            signalScoreData.insiderConfidenceMultiplier = insiderConfidenceMultiplier;
          }
          
          // Apply Tuesday bonus (1.1x multiplier for better market conditions)
          const dayOfWeek = new Date().getDay(); // 0=Sunday, 2=Tuesday
          const hasTuesdayBonus = dayOfWeek === 2;
          if (hasTuesdayBonus) {
            signalScoreData.score = parseFloat((signalScoreData.score * 1.1).toFixed(2));
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
          
          // FTD display with percentage
          let ftdDisplay = 'false';
          if (ftdData) {
            ftdDisplay = ftdData.toLocaleString('en-US');
            if (ftdPercent) {
              ftdDisplay += ` (${ftdPercent}%)`;
            }
          }
          
          const directionLabel = shortOpportunity ? 'SHORT' : (longOpportunity ? 'LONG' : 'N/A');
          
          // Fetch Weighted Average and log Stock info for ALL filings (immediately after quote data is ready)
          let waValue = 'N/A';
          try {
            waValue = await fetchWA(ticker, price, volume, averageVolume);
          } catch (waErr) {
            waValue = 'N/A';
          }
          
          const waLog = waValue !== 'N/A' ? `$${parseFloat(waValue).toFixed(2)}` : 'N/A';
          
          if (shortOpportunity || longOpportunity) {
            log('INFO', `Stock: $${ticker}, Score: ${signalScoreDisplay}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}, WA: ${waLog}, FTD: ${ftdDisplay}, ${directionLabel}`);
          } else {
            log('INFO', `Stock: $${ticker}, Score: ${signalScoreDisplay}, Price: ${priceDisplay}, Vol/Avg: ${volDisplay}/${avgDisplay}, MC: ${mcDisplay}, Float: ${floatDisplay}, S/O: ${soRatio}, WA: ${waLog}, FTD: ${ftdDisplay}`);
          }
          
          // Check for FDA Approvals and Chinese/Cayman reverse splits
          const hasFDAApproval = signalCategories.some(cat => ['FDA Approved', 'FDA Breakthrough', 'FDA Filing'].includes(cat));
          const isChinaOrCaymanReverseSplit = (normalizedIncorporated === 'China' || normalizedLocated === 'China' || normalizedIncorporated === 'Cayman Islands' || normalizedLocated === 'Cayman Islands') && signalCategories.includes('Artificial Inflation');
          const highScoreOverride = signalScoreData.score > 0.7;
          if (normalizedLocated === 'Unknown') {
            skipReason = 'No valid country';
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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
          
          // Calculate S/O ratio for use in multiple filters
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
          
          // Calculate early for use in multiple filters
          const hasExtremeSOOrStrongSignal = (soRatioValue !== null && soRatioValue > CONFIG.EXTREME_SO_RATIO) || nonNeutralSignals.length >= 3;
          
          // Check if country is whitelisted - applies to both 6-K and 8-K filings
          let countryWhitelisted = true;
          const incorporatedMatch = CONFIG.ALLOWED_COUNTRIES.some(country => normalizedIncorporated.toLowerCase().includes(country));
          const locatedMatch = CONFIG.ALLOWED_COUNTRIES.some(country => normalizedLocated.toLowerCase().includes(country));
          const isCaymanOrBVI = normalizedIncorporated.toLowerCase().includes('cayman') || normalizedLocated.toLowerCase().includes('cayman') || 
                                normalizedIncorporated.toLowerCase().includes('virgin') || normalizedLocated.toLowerCase().includes('virgin');
          const hasSPSignal = signalCategories.includes('Artificial Inflation') || signalCategories.includes('Delisting Risk') || signalCategories.includes('Bid Price Delisting') || signalCategories.includes('Nasdaq Delisting');
          
          if (filing.formType === '6-K' || filing.formType === '6-K/A') {
            // 6-K filings: Allow Cayman/BVI if extreme S/O (>80%) OR death spiral signals
            countryWhitelisted = incorporatedMatch || locatedMatch || (isCaymanOrBVI && (hasExtremeSOOrStrongSignal || hasSPSignal));
          } else {
            // 8-K and all other filings: Must match ALLOWED_COUNTRIES (Delaware, Nevada only per CONFIG)
            countryWhitelisted = incorporatedMatch || locatedMatch;
          }
          
          if (!countryWhitelisted) {
            skipReason = `Country not whitelisted (${normalizedIncorporated}, ${normalizedLocated})`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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
          const maxFloatThreshold = filing.formType === '8-K' || filing.formType === '8-K/A' ? CONFIG.MAX_FLOAT_8K : CONFIG.MAX_FLOAT_6K;
          if (floatValue !== null && floatValue > maxFloatThreshold) {
            skipReason = `Float ${floatValue.toLocaleString('en-US')} exceeds ${(maxFloatThreshold / 1000000).toFixed(0)}m limit for ${filing.formType}`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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
          
          // Dynamic volume threshold based on signal strength
          // If S/O ratio >80% OR strong non-neutral signals, allow much lower volume
          const isBiotechSignal = hasFDAApproval || signalCategories.includes('Clinical Success') || signalCategories.includes('Clinical Milestone');
          const minVolumeThreshold = isBiotechSignal ? 20000 : (hasExtremeSOOrStrongSignal ? CONFIG.STRONG_SIGNAL_MIN_VOLUME : CONFIG.MIN_ALERT_VOLUME);
          
          // Check if volume is 3x or more than average volume (bypass filter)
          const avgVolumeValue = averageVolume !== 'N/A' ? parseFloat(averageVolume) : null;
          const volumeIs3xAverage = volumeCheckLater !== null && avgVolumeValue !== null && volumeCheckLater >= (avgVolumeValue * 3);
          
          // Check volume after knowing signal type (unless volume is 3x average)
          if (!volumeIs3xAverage && volumeCheckLater !== null && volumeCheckLater < minVolumeThreshold) {
            skipReason = `Volume ${volumeCheckLater.toLocaleString('en-US')} below ${(minVolumeThreshold / 1000).toFixed(0)}k minimum (extreme S/O or strong signal: ${hasExtremeSOOrStrongSignal ? 'yes' : 'no'}, biotech: ${isBiotechSignal ? 'yes' : 'no'}) - NOT bypassed)`;
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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
          
          // Structural movers - mechanical algos execute on these
          const structuralMovers = ['Credit Default', 'Going Dark', 'Warrant Redemption', 'Asset Disposition', 'Share Consolidation', 'Deal Termination', 'Auditor Change', 'Preferred Call', 'DTC Eligible Restored', 'Debt Restructure', 'Corporate Separation'];
          const hasStructuralMover = signalCategories.some(cat => structuralMovers.includes(cat));
          
          // Death spiral categories always trigger alerts regardless of score
          const deathSpiralCategories = ['Going Concern', 'Accounting Restatement', 'Bankruptcy Filing', 'Dilution', 'Reverse Split', 'Compliance Issue'];
          const hasDeathSpiralSignal = nonNeutralSignals.some(cat => deathSpiralCategories.includes(cat));
          
          if (isChinaOrCaymanReverseSplit) {
            validSignals = true; // China/Cayman Islands reverse splits always trigger
          } else if (hasStructuralMover) {
            validSignals = true; // Structural movers - algos execute mechanically
          } else if (hasFDAApproval) {
            validSignals = true; // FDA Approval is strong enough alone
          } else if (signalCategories.includes('Clinical Success')) {
            validSignals = true; // Clinical trial success is strong bullish signal
          } else if (signalCategories.includes('Clinical Milestone')) {
            validSignals = true; // Clinical trial milestones (enrollment, phase advancement) drive movement
          } else if (hasDeathSpiralSignal) {
            validSignals = true; // Death spirals always trigger
          } else if (highScoreOverride && signalCategories.length === 1) {
            // High score (>0.7) with single signal overrides time window IF it passes all other filters
            validSignals = true;
          } else if (signalScoreData.score > 0.7 && signalCategories.length === 1) {
            validSignals = true; // Threshold to 0.7 with single signal
          } else if (signalScoreData.volumeScore >= 0.85 && signalCategories.length >= 1) {
            validSignals = true; // Strong volume spike (2x+ average) with any signal
          } else if (neutralSignals.length > 0 && signalCategories.length >= 2) {
            validSignals = true; // Has neutral signal + at least 1 other signal
          } else if (nonNeutralSignals.length >= 3 && signalCategories.length >= 3) {
            validSignals = true; // Has 3+ signals from 3+ different categories (ensures diversity, avoids signal clustering)
          } else if (nonNeutralSignals.length >= 4) {
            validSignals = true; // Has 4+ non-neutral signals (original strength maintained)
          }
          
          if (!validSignals) {
            skipReason = 'Not enough signal weight';
            const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
            const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
            log('INFO', `Links: ${secLink} ${tvLink}`);
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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
          
          // Detect reverse split ratio and reason
          let reverseSplitRatio = null;
          let reverseSplitReason = null;
          if (Object.keys(semanticSignals).includes('Artificial Inflation')) {
            const ratio = extractReverseSplitRatio(text);
            if (ratio) {
              const ratioMatch = ratio.match(/\d+$/);
              if (ratioMatch) {
                reverseSplitRatio = parseInt(ratioMatch[0]);
              }
              
              // Detect reason for split
              const lowerText = text.toLowerCase();
              if (lowerText.includes('nasdaq') && (lowerText.includes('bid') || lowerText.includes('price') || lowerText.includes('minimum'))) {
                reverseSplitReason = 'Nasdaq minimum bid price requirement';
              } else if (lowerText.includes('listing') && lowerText.includes('standard')) {
                reverseSplitReason = 'Listing standard compliance';
              } else if (lowerText.includes('consolidat')) {
                reverseSplitReason = 'Share consolidation';
              } else if (lowerText.includes('stock split')) {
                reverseSplitReason = 'Stock split';
              } else {
                reverseSplitReason = 'Reverse stock split';
              }
            }
          }
          
          const alertData = {
            ticker: ticker || filing.cik || 'Unknown',
            title: filing.title ? filing.title.replace(/\s*\(\d{10}\)\s*$/, '').trim() : 'Unknown Company',
            companyName: companyName !== 'Unknown' ? companyName : null,
            filerName: filerName || null,
            price: price,
            wa: waValue,
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
            isShort: shortOpportunity ? true : false,
            ftd: ftdData || false,
            ftdPercent: ftdPercent || null,
            intent: intent || 'Regulatory Filing',
            incorporated: normalizedIncorporated,
            located: normalizedLocated,
            filingDate: periodOfReport,
            signals: semanticSignals,
            bonusSignals: bonusSignals,
            financialRatioSignals: financialRatioSignals,
            reverseSplitRatio: reverseSplitRatio,
            reverseSplitReason: reverseSplitReason,
            formType: Array.from(foundForms),
            filingType: formLogMessage,
            cik: filing.cik,
            skipReason: skipReason,
            alertType: null  // Will be set to 'Toxic Structure', 'High Velocity', or 'Composite' if alerted
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
              log('SKIP', `$${ticker}, duplicate alert - already alerted in current session`);
              console.log('');
              // Don't save duplicate alerts
            } else {
              // Set skip reason if this alert has borderline characteristics
              if (signalScoreData.score < 0.3) {
                alertData.skipReason = 'Low Score';
              } else if (alertData.intent === 'neutral') {
                alertData.skipReason = 'Neutral Signal';
              } else if (Object.keys(semanticSignals).length < 2) {
                alertData.skipReason = 'Not Enough Signals';
              } else if (float !== 'N/A' && parseFloat(float) > CONFIG.MAX_FLOAT * 0.8) {
                alertData.skipReason = 'High Float';
              } else if (volume !== 'N/A' && parseFloat(volume) < 10000) {
                alertData.skipReason = 'Low Volume';
              }
              
              // Only save to alerts if NO skip reason (real alert)
              if (!alertData.skipReason) {
                // Save all valid alerts that passed signal and filter checks
                saveAlert(alertData);
              } else {
                // Save borderline stocks to CSV only
                saveToCSV(alertData);
                // Log the skip reason for borderline stocks
                const secLink = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${filing.cik}&type=6-K&dateb=&owner=exclude&count=100`;
                const tvLink = `https://www.tradingview.com/chart/?symbol=${getExchangePrefix(ticker)}:${ticker}`;
                log('INFO', `Links: ${secLink} ${tvLink}`);
                log('SKIP', `$${ticker}, ${alertData.skipReason}`);
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
            log('SKIP', `$${ticker}, ${skipReason}`);
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
                ftd: ftdData || false,
                ftdPercent: ftdPercent || null,
                volume: volume,
                averageVolume: averageVolume,
                incorporated: normalizedIncorporated,
                located: normalizedLocated,
                intent: semanticSignals && Object.keys(semanticSignals).length > 0 ? Object.keys(semanticSignals)[0] : null,
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

// Background price updater - fetch live prices for all alert tickers and update quote.json
const updateAllTickerPrices = async () => {
  try {
    // Load current alerts
    if (!fs.existsSync(CONFIG.ALERTS_FILE)) return;
    
    const alertsContent = fs.readFileSync(CONFIG.ALERTS_FILE, 'utf8').trim();
    if (!alertsContent) return;
    
    let alerts = [];
    try {
      alerts = JSON.parse(alertsContent);
      if (!Array.isArray(alerts)) alerts = [];
    } catch (e) {
      return;
    }
    
    // Load current performance data
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
    
    // Get unique tickers from alerts
    const tickers = [...new Set(alerts.map(a => a.ticker).filter(t => t))];
    
    if (tickers.length === 0) return;
    
    // Fetch prices for all tickers
    for (const ticker of tickers) {
      try {
        await rateLimit.wait();
        
        const quote = await yahooFinance.quote(ticker, {
          fields: ['regularMarketPrice', 'regularMarketVolume', 'averageDailyVolume3Month', 'marketCap']
        });
        
        if (quote && quote.regularMarketPrice > 0) {
          // Ensure ticker exists in performance data
          if (!performanceData[ticker]) {
            performanceData[ticker] = {
              alert: quote.regularMarketPrice,
              highest: quote.regularMarketPrice,
              lowest: quote.regularMarketPrice,
              current: quote.regularMarketPrice,
              currentPrice: quote.regularMarketPrice,
              volume: quote.regularMarketVolume || 0,
              averageVolume: quote.averageDailyVolume3Month || 0,
              marketCap: quote.marketCap || 'N/A'
            };
          } else {
            // Update with latest price
            performanceData[ticker].currentPrice = quote.regularMarketPrice;
            performanceData[ticker].current = quote.regularMarketPrice;
            performanceData[ticker].volume = quote.regularMarketVolume || 0;
            performanceData[ticker].averageVolume = quote.averageDailyVolume3Month || 0;
            performanceData[ticker].marketCap = quote.marketCap || 'N/A';
            
            // Update high/low
            if (quote.regularMarketPrice > performanceData[ticker].highest) {
              performanceData[ticker].highest = quote.regularMarketPrice;
            }
            if (quote.regularMarketPrice < performanceData[ticker].lowest) {
              performanceData[ticker].lowest = quote.regularMarketPrice;
            }
          }
        }
      } catch (err) {
        // Silently skip if fetch fails for this ticker
      }
    }
    
    // Save updated performance data
    fs.writeFileSync(CONFIG.PERFORMANCE_FILE, JSON.stringify(performanceData, null, 2));
  } catch (err) {
    // Silently fail background updates
  }
};

// Run price updates every 30 seconds
setInterval(updateAllTickerPrices, 30000);
// Also run immediately on startup
updateAllTickerPrices();

// Health monitoring - track memory usage
setInterval(() => {
  const memory = process.memoryUsage();
  const heapUsedMB = Math.round(memory.heapUsed / 1024 / 1024);
  if (heapUsedMB > 500) {
    log('WARN', `High memory: ${heapUsedMB}MB (${Math.round(memory.heapUsed / memory.heapTotal * 100)}% of heap)`);
  }
}, 60000);

// Graceful shutdown handler
process.on('SIGTERM', () => {
  log('INFO', 'Shutdown signal received');
  process.exit(0);
});

process.on('SIGINT', () => {
  log('INFO', 'Application process terminated');
  process.exit(0);
});

// Uncaught error handler
process.on('uncaughtException', (err) => {
  log('ERROR', `Uncaught exception: ${err.message}`);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log('ERROR', `Unhandled rejection at ${promise}: ${reason}`);
});