exports.hello = (req, res) => {
  res.send(`Hello ${req.body.name || 'World'}!`);
};
