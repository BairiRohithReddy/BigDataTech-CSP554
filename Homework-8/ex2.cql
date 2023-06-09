CREATE TABLE A20526972.Music(
    artistName text,
    albumName text,
    numberSold int,
    Cost int,
    PRIMARY KEY(artistName, albumName)
) WITH CLUSTERING ORDER BY(albumName DESC);
