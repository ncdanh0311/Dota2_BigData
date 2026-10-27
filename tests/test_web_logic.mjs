// Optional local / CI unit check. Node is not used to build or run the website.
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
const source=await readFile(new URL('../web/static/logic.js',import.meta.url),'utf8');
const {rankRows,eligibleRows,pageRows,validDraft}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
const rows=[{hero_id:3,picks:100,win_rate:.6,sample_status:'eligible'},
            {hero_id:1,picks:100,win_rate:.6,sample_status:'eligible'},
            {hero_id:2,picks:99,win_rate:.9,sample_status:'insufficient_sample'},
            {hero_id:4,picks:200,win_rate:.6,sample_status:'eligible'}];
assert.deepEqual(rankRows(eligibleRows(rows,false),'win_rate','picks',['hero_id']).map(r=>r.hero_id),[4,1,3]);
assert.equal(eligibleRows(rows,true).length,4);
assert.deepEqual(rows.map(r=>r.hero_id),[3,1,2,4]); // Sorting cannot mutate source Gold.
assert.equal(pageRows(rows,9,2).page,2);
assert.equal(pageRows(rows,1,2).total,4);
assert.deepEqual(pageRows([],99).rows,[]);
assert.equal(validDraft([1,2,3,4,5],[6,7,8,9,10]),true);
assert.equal(validDraft([1,2,3,4,5],[6,7,8,9,1]),false);
assert.equal(validDraft([null,2,3,4,5],[6,7,8,9,10]),false);
const evidence=new URL('../evidence/step5/',import.meta.url);
await mkdir(evidence,{recursive:true});
await writeFile(new URL('display_tests.json',evidence),JSON.stringify({status:'PASS',source:'synthetic JavaScript fixtures',
  checks:['eligible sample filter','deterministic ranking ties','immutable source rows','pagination','ten unique draft heroes']},null,2)+'\n');
console.log('Web display rules passed.');
