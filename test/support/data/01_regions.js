module.exports = {
  name: 'sqb_test.regions',
  createSql: (`
CREATE TABLE sqb_test.regions
(
    id character varying(10) COLLATE pg_catalog."default" NOT NULL,
    name character varying(16) COLLATE pg_catalog."default",
    CONSTRAINT regions_pkey PRIMARY KEY (id)
)
  `),
  rows: [
    {
      ID: 'FR',
      Name: 'FR Region'
    },
    {
      ID: 'TR',
      Name: 'TR Region'
    },
    {
      ID: 'GB',
      Name: 'GB Region'
    },
    {
      ID: 'US',
      Name: 'US Region'
    },
    {
      ID: 'CN',
      Name: 'CN Region'
    }
  ]
};