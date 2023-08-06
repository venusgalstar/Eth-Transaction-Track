

"""
The following SQLite update is likely necessary to properly track everything.
Think first about alternative approaches and what we actually want to track!
Maybe a different SQLite schema is necessary to track all transfers and swaps.

'contractTo': '',
'contractValue': ''

replace with

'contractFrom'
'contractTo'
'contractValue0'
'contractValue1'
'contractEvent' (transfer / swap)


https://scan.pulsechain.com/tx/0xf199cb56bc49ed946522e2a10fd7016ee475ef2ac4a30efb39febaa479dcf847 (swap)

(maybe just track through same transfer within a single block)
"""

# AachenTwo!