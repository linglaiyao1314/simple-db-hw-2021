package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile { // 存储将文件转换成HeapPage
    private File f;
    private TupleDesc td;
    private HeapPage[] pages;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.pages = new HeapPage[numPages()];

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // 标识文件唯一id
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNumber = pid.getPageNumber();
        Page page = null;
        try
        {
            int offset = pageNumber * BufferPool.getPageSize();
            int bufSize = BufferPool.getPageSize();
            byte[] pageData = new byte[bufSize];
            FileInputStream fileInputStream = new FileInputStream(this.getFile());
            // 跳过已读取的页
            fileInputStream.skip(offset);
            // 读取当前页数据
            fileInputStream.read(pageData);
            page = new HeapPage((HeapPageId) pid, pageData);
            fileInputStream.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // 通过每页的大小计算HeapFile能存储多少页
        int pages = Math.toIntExact(getFile().length() / BufferPool.getPageSize());
        if(pages > BufferPool.DEFAULT_PAGES){
            pages = BufferPool.DEFAULT_PAGES;
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator{
        private HeapFile hf;
        private boolean open;
        private int pageNo;
        private Iterator<Tuple> currentTupleIterator;
        private TransactionId tid;

        public HeapFileIterator(HeapFile hf, TransactionId tid){
            this.hf = hf;
            this.open = false;
            this.pageNo = 0;
            this.currentTupleIterator = null;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.open = true;
        }

        public Iterator<Tuple> getNewIterator(){
            if(pageNo >= this.hf.numPages()){
                return null;
            }
            HeapPageId pid = new HeapPageId(this.hf.getId(), pageNo);
            try {
                // 从buffer pool中获取已缓存页
                Page cachePage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                if(cachePage != null){
                    return ((HeapPage)cachePage).iterator();
                }
            }catch (Exception e){
                HeapPage page = (HeapPage)this.hf.readPage(pid);
                if(page != null){
                    return page.iterator();
                }
            }
            return null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!open){
                return false;
            }
            if(currentTupleIterator == null){
                currentTupleIterator = getNewIterator();
            }
            if(currentTupleIterator.hasNext()){
               return true;
            }else{
                this.pageNo += 1;
                currentTupleIterator = getNewIterator();
            }
            if(currentTupleIterator == null){
                return false;
            }
            return currentTupleIterator.hasNext();

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!open){
                throw new NoSuchElementException("no such item");
            }
            return currentTupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNo = 0;
            currentTupleIterator = null;
        }

        @Override
        public void close() {
            this.open = false;
        }
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this, tid);
    }

}

