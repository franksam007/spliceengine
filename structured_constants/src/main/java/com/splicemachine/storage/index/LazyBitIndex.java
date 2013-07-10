package com.splicemachine.storage.index;

import com.splicemachine.storage.BitReader;

import java.util.BitSet;

/**
 * BitIndex which lazily decodes entries as needed, and which does not re-encode entries.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
abstract class LazyBitIndex implements BitIndex{
    protected BitSet decodedBits;

    protected byte[] encodedBitMap;
    protected int offset;
    protected int length;
    protected BitReader bitReader;

    protected LazyBitIndex(byte[] encodedBitMap,int offset,int length,int bitPos){
        this.encodedBitMap = encodedBitMap;
        this.offset = offset;
        this.length = length;
        this.decodedBits = new BitSet();
        this.bitReader = new BitReader(encodedBitMap,offset,length,bitPos);
    }

    @Override
    public int length() {
        decodeAll();
        return decodedBits.length();
    }

    private void decodeAll() {
        int next;
        while((next = decodeNext()) >=0){
            decodedBits.set(next);
        }
    }

    protected abstract int decodeNext();

    @Override
    public boolean isSet(int pos) {
        decodeUntil(pos);
        return decodedBits.get(pos);
    }

    private void decodeUntil(int pos) {
        while(decodedBits.length()<pos+1){
            int next = decodeNext();
            if(next <0) return; //out of data to decode

            decodedBits.set(next);
        }
    }

    @Override
    public int nextSetBit(int position) {
        //try and get the next item out of decodedBits first
        int val = decodedBits.nextSetBit(position);
        if(val>=0) return val;

        //try decoding some more
        int i;
        do{
            i = decodeNext();
            if(i<0) break;

            decodedBits.set(i);
            if(i>position)
                return i;
        }while(i>=0);

        //couldn't find any entries
        return -1;
    }

    @Override
    public int cardinality() {
        decodeAll();
        return decodedBits.cardinality();
    }

    @Override
    public int cardinality(int position) {
        decodeUntil(position);
        int count=0;
        for(int i=decodedBits.nextSetBit(0);i>=0 && i<position;i=decodedBits.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        decodeUntil(bitSet.length());
        return decodedBits.intersects(bitSet);
    }

    @Override
    public boolean isEmpty() {
        decodeAll();
        return decodedBits.isEmpty();
    }

    @Override
    public BitSet and(BitSet bitSet) {
        decodeUntil(bitSet.length());
        final BitSet bits = (BitSet) decodedBits.clone();
        bits.and(bitSet);
        return bits;
    }

    @Override
    public byte[] encode() {
        byte[] copy = new byte[length];
        System.arraycopy(encodedBitMap,offset,copy,0,copy.length);
        return copy;
    }

    @Override
    public int encodedSize() {
        return length;
    }
}

