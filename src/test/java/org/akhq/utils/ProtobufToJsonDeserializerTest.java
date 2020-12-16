package org.akhq.utils;

import com.google.protobuf.Any;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.akhq.configs.Connection.ProtobufDeserializationTopicsMapping;
import org.akhq.configs.TopicsMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ProtobufToJsonDeserializerTest {
    ProtobufDeserializationTopicsMapping protobufDeserializationTopicsMapping;
    AlbumProto.Album albumProto;
    FilmProto.Film filmProto;
    ComplexWithBookOutsideProto.ComplexWithBookOutside complexProtobufObjectWithBookOutside;
    ComplexWithBookInsideProto.ComplexWithBookInside complexProtobufObjectWithBookInside;
    MultipleLayerComplexProto.MultipleLayerComplex multipleLayerComplex;


    @BeforeEach
    public void before() throws URISyntaxException, IOException {
        createTopicProtobufDeserializationMapping();
        createAlbumObject();
        createFilmObject();
        createComplexObjectWithBookOutside();
        createComplexObjectWithBookInside();
        createMultipleLayerComplex();
    }

    private void createTopicProtobufDeserializationMapping() throws URISyntaxException, IOException {
        protobufDeserializationTopicsMapping = new ProtobufDeserializationTopicsMapping();

        URI uri = ClassLoader.getSystemResource("protobuf_desc").toURI();
        String protobufDescriptorsFolder = Paths.get(uri).toString();
        protobufDeserializationTopicsMapping.setDescriptorsFolder(protobufDescriptorsFolder);

        TopicsMapping albumTopicsMapping = new TopicsMapping();
        albumTopicsMapping.setTopicRegex("album.*");
        String base64AlbumDescriptor = encodeDescriptorFileToBase64("album.desc");
        albumTopicsMapping.setDescriptorFileBase64(base64AlbumDescriptor);
        albumTopicsMapping.setValueMessageType("Album");

        TopicsMapping filmTopicsMapping = new TopicsMapping();
        filmTopicsMapping.setTopicRegex("film.*");
        filmTopicsMapping.setDescriptorFile("film.desc");
        filmTopicsMapping.setValueMessageType("Film");

        TopicsMapping complexObjectTopicsMapping1 = new TopicsMapping();
        complexObjectTopicsMapping1.setTopicRegex("complex1.*");
        complexObjectTopicsMapping1.setDescriptorFile("complexWithBookOutside.desc");
        complexObjectTopicsMapping1.setValueMessageType("ComplexWithBookOutside");

        TopicsMapping complexObjectTopicsMapping2 = new TopicsMapping();
        complexObjectTopicsMapping2.setTopicRegex("complex2.*");
        complexObjectTopicsMapping2.setDescriptorFile("complexWithBookInside.desc");
        complexObjectTopicsMapping2.setValueMessageType("ComplexWithBookInside");

        TopicsMapping multipleLayerComplexObjectTopicsMapping = new TopicsMapping();
        multipleLayerComplexObjectTopicsMapping.setTopicRegex("multiple.*");
        multipleLayerComplexObjectTopicsMapping.setDescriptorFile("multipleLayerComplex.desc");
        multipleLayerComplexObjectTopicsMapping.setValueMessageType("MultipleLayerComplex");

        protobufDeserializationTopicsMapping.setTopicsMapping(
                Arrays.asList(albumTopicsMapping, filmTopicsMapping,
                        complexObjectTopicsMapping1, complexObjectTopicsMapping2,
                        multipleLayerComplexObjectTopicsMapping));
    }

    private String encodeDescriptorFileToBase64(String descriptorFileName) throws URISyntaxException, IOException {
        URI uri = ClassLoader.getSystemResource("protobuf_desc").toURI();
        String protobufDescriptorsFolder = Paths.get(uri).toString();

        String fullName = protobufDescriptorsFolder + File.separator + descriptorFileName;
        byte[] descriptorFileBytes = Files.readAllBytes(Path.of(fullName));
        return Base64.getEncoder().encodeToString(descriptorFileBytes);
    }

    private void createAlbumObject() {
        List<String> artists = Collections.singletonList("Imagine Dragons");
        List<String> songTitles = Arrays.asList("Birds", "Zero", "Natural", "Machine");
        Album album = new Album("Origins", artists, 2018, songTitles);
        albumProto = AlbumProto.Album.newBuilder()
                .setTitle(album.getTitle())
                .addAllArtist(album.getArtists())
                .setReleaseYear(album.getReleaseYear())
                .addAllSongTitle(album.getSongsTitles())
                .build();
    }

    private void createFilmObject() {
        List<String> starring = Arrays.asList("Harrison Ford", "Mark Hamill", "Carrie Fisher", "Adam Driver", "Daisy Ridley");
        Film film = new Film("Star Wars: The Force Awakens", "J. J. Abrams", 2015, 135, starring);
        GregorianCalendar date = new GregorianCalendar(2020, Calendar.JANUARY, 16);
        date.setTimeZone(TimeZone.getTimeZone("GMT"));
        filmProto = FilmProto.Film.newBuilder()
                .setName(film.getName())
                .setProducer(film.getProducer())
                .setReleaseYear(film.getReleaseYear())
                .setDuration(film.getDuration())
                .addAllStarring(film.getStarring())
                .setTimestamp(Any.pack(Timestamp.newBuilder()
                        .setSeconds(date.getTime().getTime() / 1000)
                        .build()))
                .build();
    }

    private void createComplexObjectWithBookOutside() {
        BookProto.Book bookProto = BookProto.Book.newBuilder()
                .setTitle("Les Miserables")
                .setAuthor("Victor Hugo")
                .setPrice(DoubleValue.newBuilder().setValue(123d))
                .build();
        complexProtobufObjectWithBookOutside = ComplexWithBookOutsideProto.ComplexWithBookOutside.newBuilder()
                .setAlbum(albumProto)
                .setFilm(filmProto)
                .setAnything(Any.pack(bookProto))
                .setStringWrapper(StringValue.newBuilder()
                        .setValue("Book message type described in outside proto-file").build())
                .build();
    }

    private void createComplexObjectWithBookInside() {
        BookProto.Book bookProto = BookProto.Book.newBuilder()
                .setTitle("Les Miserables")
                .setAuthor("Victor Hugo")
                .setPrice(DoubleValue.newBuilder().setValue(123d))
                .build();
        complexProtobufObjectWithBookInside = ComplexWithBookInsideProto.ComplexWithBookInside.newBuilder()
                .setAlbum(albumProto)
                .setFilm(filmProto)
                .setAnything(Any.pack(bookProto))
                .setStringWrapper(StringValue.newBuilder()
                        .setValue("Book message type described inside complex object proto-file").build())
                .build();
    }

    private void createMultipleLayerComplex() {
        multipleLayerComplex = MultipleLayerComplexProto.MultipleLayerComplex.newBuilder()
                .setAlbum(albumProto)
                .setFilm(filmProto)
                .setComplex(complexProtobufObjectWithBookOutside)
                .build();
    }

    @Test
    public void deserializeAlbum() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryAlbum = albumProto.toByteArray();
        String decodedAlbum = protobufToJsonDeserializer.deserialize("album.topic.name", binaryAlbum, false);
        String expectedAlbum = "{\n" +
                "  \"title\": \"Origins\",\n" +
                "  \"artist\": [\"Imagine Dragons\"],\n" +
                "  \"releaseYear\": 2018,\n" +
                "  \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "}";
        assertEquals(expectedAlbum, decodedAlbum);
    }

    @Test
    public void deserializeFilm() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryFilm = filmProto.toByteArray();
        String decodedFilm = protobufToJsonDeserializer.deserialize("film.topic.name", binaryFilm, false);
        String expectedFilm = "{\n" +
                "  \"name\": \"Star Wars: The Force Awakens\",\n" +
                "  \"producer\": \"J. J. Abrams\",\n" +
                "  \"releaseYear\": 2015,\n" +
                "  \"duration\": 135,\n" +
                "  \"starring\": [\"Harrison Ford\", \"Mark Hamill\", \"Carrie Fisher\", \"Adam Driver\", \"Daisy Ridley\"],\n" +
                "  \"timestamp\": {\n" +
                "    \"@type\": \"type.googleapis.com/google.protobuf.Timestamp\",\n" +
                "    \"value\": \"2020-01-16T00:00:00Z\"\n" +
                "  }\n" +
                "}";
        assertEquals(expectedFilm, decodedFilm);
    }

    @Test
    public void deserializeForNotMatchingTopic() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryFilm = filmProto.toByteArray();
        String decodedFilm = protobufToJsonDeserializer.deserialize("random.topic.name", binaryFilm, false);
        assertNull(decodedFilm);
    }

    @Test
    public void deserializeForKeyWhenItsTypeNotSet() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryFilm = filmProto.toByteArray();
        Exception exception = assertThrows(RuntimeException.class, () -> {
            protobufToJsonDeserializer.deserialize("film.topic.name", binaryFilm, true);
        });
        String expectedMessage = "message type is not specified neither for a key, nor for a value";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void deserializeComplexObjectWithBookDescribedInOutsideFile() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryComplexObject = complexProtobufObjectWithBookOutside.toByteArray();
        String decodedComplexObject = protobufToJsonDeserializer.deserialize("complex1.topic.name", binaryComplexObject, false);
        String expectedComplexObject = "{\n" +
                "  \"album\": {\n" +
                "    \"title\": \"Origins\",\n" +
                "    \"artist\": [\"Imagine Dragons\"],\n" +
                "    \"releaseYear\": 2018,\n" +
                "    \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "  },\n" +
                "  \"film\": {\n" +
                "    \"name\": \"Star Wars: The Force Awakens\",\n" +
                "    \"producer\": \"J. J. Abrams\",\n" +
                "    \"releaseYear\": 2015,\n" +
                "    \"duration\": 135,\n" +
                "    \"starring\": [\"Harrison Ford\", \"Mark Hamill\", \"Carrie Fisher\", \"Adam Driver\", \"Daisy Ridley\"]\n" +
                "  },\n" +
                "  \"stringWrapper\": \"Book message type described in outside proto-file\",\n" +
                "  \"anything\": {\n" +
                "    \"@type\": \"type.googleapis.com/org.akhq.utils.Book\",\n" +
                "    \"title\": \"Les Miserables\",\n" +
                "    \"author\": \"Victor Hugo\",\n" +
                "    \"price\": 123.0\n" +
                "  }\n" +
                "}";
        assertEquals(expectedComplexObject, decodedComplexObject);
    }

    @Test
    public void deserializeComplexObjectWithBookDescribedInside() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryComplexObject = complexProtobufObjectWithBookInside.toByteArray();
        String decodedComplexObject = protobufToJsonDeserializer.deserialize("complex2.topic.name", binaryComplexObject, false);
        String expectedComplexObject = "{\n" +
                "  \"album\": {\n" +
                "    \"title\": \"Origins\",\n" +
                "    \"artist\": [\"Imagine Dragons\"],\n" +
                "    \"releaseYear\": 2018,\n" +
                "    \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "  },\n" +
                "  \"film\": {\n" +
                "    \"name\": \"Star Wars: The Force Awakens\",\n" +
                "    \"producer\": \"J. J. Abrams\",\n" +
                "    \"releaseYear\": 2015,\n" +
                "    \"duration\": 135,\n" +
                "    \"starring\": [\"Harrison Ford\", \"Mark Hamill\", \"Carrie Fisher\", \"Adam Driver\", \"Daisy Ridley\"]\n" +
                "  },\n" +
                "  \"stringWrapper\": \"Book message type described inside complex object proto-file\",\n" +
                "  \"anything\": {\n" +
                "    \"@type\": \"type.googleapis.com/org.akhq.utils.Book\",\n" +
                "    \"title\": \"Les Miserables\",\n" +
                "    \"author\": \"Victor Hugo\",\n" +
                "    \"price\": 123.0\n" +
                "  }\n" +
                "}";
        assertEquals(expectedComplexObject, decodedComplexObject);
    }

    @Test
    public void deserializeMultipleLayerComplex() {
        ProtobufToJsonDeserializer protobufToJsonDeserializer = new ProtobufToJsonDeserializer(protobufDeserializationTopicsMapping);
        final byte[] binaryMultipleLayerComplexObject = multipleLayerComplex.toByteArray();
        String decodedMultipleLayerComplexObject = protobufToJsonDeserializer.deserialize("multiple.complex.topic.name", binaryMultipleLayerComplexObject, false);
        String expectedMultipleLayerComplexObject = "{\n" +
                "  \"album\": {\n" +
                "    \"title\": \"Origins\",\n" +
                "    \"artist\": [\"Imagine Dragons\"],\n" +
                "    \"releaseYear\": 2018,\n" +
                "    \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "  },\n" +
                "  \"film\": {\n" +
                "    \"name\": \"Star Wars: The Force Awakens\",\n" +
                "    \"producer\": \"J. J. Abrams\",\n" +
                "    \"releaseYear\": 2015,\n" +
                "    \"duration\": 135,\n" +
                "    \"starring\": [\"Harrison Ford\", \"Mark Hamill\", \"Carrie Fisher\", \"Adam Driver\", \"Daisy Ridley\"]\n" +
                "  },\n" +
                "  \"complex\": {\n" +
                "    \"album\": {\n" +
                "      \"title\": \"Origins\",\n" +
                "      \"artist\": [\"Imagine Dragons\"],\n" +
                "      \"releaseYear\": 2018,\n" +
                "      \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "    },\n" +
                "    \"film\": {\n" +
                "      \"name\": \"Star Wars: The Force Awakens\",\n" +
                "      \"producer\": \"J. J. Abrams\",\n" +
                "      \"releaseYear\": 2015,\n" +
                "      \"duration\": 135,\n" +
                "      \"starring\": [\"Harrison Ford\", \"Mark Hamill\", \"Carrie Fisher\", \"Adam Driver\", \"Daisy Ridley\"]\n" +
                "    },\n" +
                "    \"stringWrapper\": \"Book message type described in outside proto-file\",\n" +
                "    \"anything\": {\n" +
                "      \"@type\": \"type.googleapis.com/org.akhq.utils.Book\",\n" +
                "      \"title\": \"Les Miserables\",\n" +
                "      \"author\": \"Victor Hugo\",\n" +
                "      \"price\": 123.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        assertEquals(expectedMultipleLayerComplexObject, decodedMultipleLayerComplexObject);
    }
}
