import java.util.UUID;

public class Book {
    private UUID id;
    private String title;
    private String author;

    Book(){
    }

    public Book(UUID id, String title, String author){
        this.id = id;
        this.title = title;
        this.author = author;
    }

    public UUID getId(){
        return id;
    }

    public void setID(UUID id){
        this.id = id;
    }

    public String getTitle(){
        return title;
    }

    public void setTitle(String title){
        this.title = title;
    }

    public String getAuthor(){
        return author;
    }

    public void setAuthor(String authorr){
        this.author = author;
    }


}
