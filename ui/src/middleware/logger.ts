export default function loggerMiddleware(store) {
  return next => action => {
    console.log(action);
    return next(action);
  };
}