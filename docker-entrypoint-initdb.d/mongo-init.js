print('Start #################################################################');

db = db.getSiblingDB('test_db');

db.createCollection( "contacts", {
   validator: { $jsonSchema: {
      bsonType: "object",
      required: [ "phone" ],
      properties: {
         phone: {
            bsonType: "string",
            description: "must be a string and is required"
         },
         status: {
            enum: [ "Unknown", "Incomplete" ],
            description: "can only be one of the enum values"
         }
      }
   } }
} );

db.contacts.insert( { name: "Beatka", status: "Unknown" } );
db.contacts.insert( { name: "Adrian", status: "Incomplete" } );

print('END #################################################################');