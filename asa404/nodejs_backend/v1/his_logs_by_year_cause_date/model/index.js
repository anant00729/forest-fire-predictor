const { getClient } = require("../../../database/connect-database");

class Story {
  async findFireRecordsByYear(storyId) {
    let q1 = ``;

    getClient;
    try {
      let res_d = await db.query(q1, { replacements: { id: storyId } });
      if (res_d[0].length === 0) {
        return { status: false, message: "Story not Found" };
      } else {
        delete res_d[0][0].password;
        return { status: true, message: "Story Found", data: res_d[0][0] };
      }
    } catch (error) {
      return { status: false, message: error.message };
    }
  }
}

module.exports = Story;
